package sdfs

import (
	"bufio"
	"cs425_mp4/internal/config"
	"cs425_mp4/internal/core"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type FileOperationType string

const (
	GET_OP    FileOperationType = "GET"
	PUT_OP    FileOperationType = "PUT"
	DELETE_OP FileOperationType = "DELETE"
)

/*
Stores information about a File Operation request sent to the leader
*/
type FileOperationTask struct {
	FileOpType          FileOperationType
	SdfsFilename        string
	ClientLocalFilename string
	ClientNode          core.NodeID // client node that requested the operation
	NewFileSize         int64       // only used for PUT operation when PUT-ing a new file, we need to know the new filesize
	RequestedTime       int64       // unix Nano time of the requested time
}

func (task *FileOperationTask) ToString() string {
	ret := fmt.Sprintf("Operation: %s\nSDFS Filename: %s\nLocal Filename: %s\n",
		string(task.FileOpType), task.SdfsFilename, task.ClientLocalFilename)
	return ret
}

/*
Stores information about the different operations on a single file in the
SDFS system. Stores information about the current write/read operations,
a buffer of waiting write/reads to be queued later, etc.
*/
type FileOperationsMetadata struct {
	SdfsFilename string

	NeedsReReplication                    bool
	StartedReReplicationProcedure         bool
	FailedReplicasBuffer                  []core.NodeID
	CurrentlyProcessingFailedReplicasList []core.NodeID
	//ReplicationStartTime                  int64

	// buffers are the tasks waiting to be scheduled
	WriteBuffer []*FileOperationTask
	ReadBuffer  []*FileOperationTask

	// queue of current operations being executed
	CurrentWriteOps []*FileOperationTask
	CurrentReadOps  []*FileOperationTask // max of 2 allowed

	// these can't exceed 4
	NumConsecutiveWritesWithWaitingReads int // number of consecutive writes that have been executed with at least one read waiting
	NumConsecutiveReadsWithWaitingWrites int // number of consecutive reads that have been executed with at least one write waiting
}

/*
SDFSLeaderService holds information regarding the entire distributed file system, where there is
1 group and 1 leader. The leader of the group is responsible for using the SDFSLeaderService object
for keeping track of information in the group/distributed-file-system

All operations like RemoveFile and AddFile inside the SDFSLeaderService does NOT actually write the file
contents to the SDFS. SDFSLeaderService is simply meant for keeping track of metadata of the entire system,
like which nodes store which files, where shards are located, properly load balancing shards, choosing
replicas, etc. It is all about MetaData.

Data that SDFSLeaderService keeps track of:
  - Which Nodes store which SDFS files
  - Currently executing tasks
  - Currently buffered tasks (tasks waiting to run)
*/
type SDFSLeaderService struct {
	/* FileToNodes
	key: sdfs_filename
	value: map where
		key: shard index
		value: list of NodeIDs representing the replica nodes that this shard is stored in
	*/
	FileToNodes map[string]map[int][]core.NodeID // TODO: you can move this into FileOperationMetadata structure, no need to keep it separate

	// maps sdfs_filename to a FileOperationMetadata struct which holds information about the current write/read
	// operations occurring on the current sdfs file. This is used by the leader to schedule tasks,
	FileOperations     map[string]*FileOperationsMetadata
	ActiveNodes        []core.NodeID // list of alive nodes in the distributed file system
	IsRunning          bool
	DispatcherWaitTime time.Duration

	MaxNumConcurrentReads             int // MP3 spec => 2
	MaxNumConcurrentWrites            int // MP3 spec => 1
	MaxNumConsecutiveOpsWithOtherWait int // MP3 spec => 4

	MutexLock sync.Mutex
	logFile   *os.File
}

func NewSDFSLeaderService(dispatcherWaitTime time.Duration, maxNumConcurrentReads int,
	maxNumConcurrentWrites int, maxNumConsecutiveOps int, loggingFile *os.File) *SDFSLeaderService {
	nn := &SDFSLeaderService{
		ActiveNodes:                       make([]core.NodeID, 0),
		FileToNodes:                       make(map[string]map[int][]core.NodeID), // TODO: is this correctly initialized?
		FileOperations:                    make(map[string]*FileOperationsMetadata),
		IsRunning:                         false,
		MaxNumConcurrentReads:             maxNumConcurrentReads,
		MaxNumConcurrentWrites:            maxNumConcurrentWrites,
		MaxNumConsecutiveOpsWithOtherWait: maxNumConsecutiveOps,
		DispatcherWaitTime:                dispatcherWaitTime,
		logFile:                           loggingFile,
	}

	return nn
}

func (leader *SDFSLeaderService) Start() {
	leader.IsRunning = true
	go leader.dispatcher()
}

/*
This dispatcher runs on a separate goroutine. It periodically checks all its tasks on every file in the
system and sees if it can schedule a new task.

loop through all SDFS files

	for each maplejuice file, check if we can schedule a new file operation
	whenever a file operation is done, that thread will send the data to the client and it will remove the
	currently running var or whatever down 1 or make to nil or something, so that dispatcher will know that it
	is free to execute a new command
*/
func (leader *SDFSLeaderService) dispatcher() {
	for leader.IsRunning {
		leader.MutexLock.Lock()
		for _, fileOpMD := range leader.FileOperations { // loop through all files

			// get number of current write/read operations being executed & num waiting operations
			numCurrentWrites := len(fileOpMD.CurrentWriteOps)
			numCurrentReads := len(fileOpMD.CurrentReadOps)
			areReadsWaiting := len(fileOpMD.ReadBuffer) > 0
			areWritesWaiting := len(fileOpMD.WriteBuffer) > 0
			var newTask *FileOperationTask

			if fileOpMD.NeedsReReplication && !fileOpMD.StartedReReplicationProcedure { // not allowed to perform a write op during re-replication, but can read
				// empty the buffer of affected nodes into the list of currently being processed for re-replication nodes
				// and then notify one of the alive replicas to directly transfer its sdfs_file to the other node we tell it
				// if we get an ACK back then we are good, otherwise it means that node we contacted must've also failed or whatever,
				// so we will try again with another replica, until we try all existing replicas to contact them
				fileOpMD.StartedReReplicationProcedure = true
				fileOpMD.CurrentlyProcessingFailedReplicasList = make([]core.NodeID, len(fileOpMD.FailedReplicasBuffer))
				copy(fileOpMD.CurrentlyProcessingFailedReplicasList, fileOpMD.FailedReplicasBuffer)
				fileOpMD.FailedReplicasBuffer = make([]core.NodeID, 0) // empty the buffer

				leader.notifyExistingReplicasToReReplicate(fileOpMD.SdfsFilename, fileOpMD.CurrentlyProcessingFailedReplicasList)
				// TODO: future improvement: for all the files we are re-replicating, we should mark that file in the write state so that we don't have a client trying to write/read from that file until its finished
				// ^ but currently the demo doesn't test this so we don't need to implement this
			}

			if !fileOpMD.NeedsReReplication && numCurrentWrites < leader.MaxNumConcurrentWrites &&
				numCurrentReads == 0 && areWritesWaiting && fileOpMD.NumConsecutiveWritesWithWaitingReads < leader.MaxNumConsecutiveOpsWithOtherWait { // schedule WRITE

				// pop using FIFO policy from the Buffer to the list of current operations
				newTask, fileOpMD.WriteBuffer = fileOpMD.WriteBuffer[0], fileOpMD.WriteBuffer[1:]
				fileOpMD.CurrentWriteOps = append(fileOpMD.CurrentWriteOps, newTask)

				// check if there is a read waiting, if there is, then we need to increment the number of consecutive writes with read waiting var
				fileOpMD.NumConsecutiveReadsWithWaitingWrites = 0 // reset this var since we are writing now
				if areReadsWaiting {
					fileOpMD.NumConsecutiveWritesWithWaitingReads += 1
				} else {
					fileOpMD.NumConsecutiveWritesWithWaitingReads = 0
				}

				leader.notifyClientToExecuteTask(newTask)
			} else if numCurrentReads < leader.MaxNumConcurrentReads && numCurrentWrites == 0 && areReadsWaiting &&
				fileOpMD.NumConsecutiveReadsWithWaitingWrites < leader.MaxNumConsecutiveOpsWithOtherWait { // schedule READ

				// move from buffer to current read operation queue to indicate its currently running - using FIFO
				newTask, fileOpMD.ReadBuffer = fileOpMD.ReadBuffer[0], fileOpMD.ReadBuffer[1:]
				fileOpMD.CurrentReadOps = append(fileOpMD.CurrentReadOps, newTask)

				// check if there is a write waiting, if there is, then we need to increment the number of consecutive reads with write waiting var
				fileOpMD.NumConsecutiveWritesWithWaitingReads = 0 // reset this var since we are reading now
				if areWritesWaiting {
					fileOpMD.NumConsecutiveReadsWithWaitingWrites += 1
				} else {
					fileOpMD.NumConsecutiveReadsWithWaitingWrites = 0
				}

				leader.notifyClientToExecuteTask(newTask)
			}
		}
		leader.MutexLock.Unlock()

		time.Sleep(leader.DispatcherWaitTime)
	}
}

func (leader *SDFSLeaderService) notifyExistingReplicasToReReplicate(sdfs_filename string, currentlyFailedReplicas []core.NodeID) {
	numNewReplicasRequired := len(currentlyFailedReplicas)
	//fmt.Printf("Num New Replicas Required: %d\n", numNewReplicasRequired)

	// get list of existing replicas that we can ask to perform the re-replication
	existingReplicasToAsk := leader.FileToNodes[sdfs_filename][0]
	newPossibleReplicas := make([]core.NodeID, 0) // stores all possible - we just wanna send all and let the client decide

	// choose how many ever extra nodes you need and choose those
	for _, nodeId := range leader.ActiveNodes {
		// make sure its not one of the existing replicas - we want a new unique replica
		if !leader.nodeExistsInNodeIDSlice(existingReplicasToAsk, nodeId) {
			newPossibleReplicas = append(newPossibleReplicas, nodeId)
		}
	}

	// now contact the existingReplicasToAsk one at a time asking them if they can re-replicate
	// to newReplicas. If they return with a successful ACK, then we chillin, otherwise, we need to ask another
	// existingReplicaToAsk
	//leader.FileOperations[sdfs_filename].ReplicationStartTime = time.Now().UnixNano()
	startTime := time.Now().UnixNano()

	for _, exisitngReplica := range existingReplicasToAsk {
		//fmt.Println("Contacting an existing replica node to re-replicate sdfs_filename")
		replicaConn, err1 := net.Dial("tcp", exisitngReplica.IpAddress+":"+exisitngReplica.SDFSServerPort)
		replicaReader := bufio.NewReader(replicaConn)
		if err1 != nil {
			//fmt.Println("Failed to contact this replica node. Going to try another replica node")
			continue
		}

		//logMessageHelper(leader.logFile, "Sending ReReplicate request")
		SendReReplicateRequest(replicaConn, sdfs_filename, newPossibleReplicas, numNewReplicasRequired)

		// NOTE: ideally this should be in a separate thread/goroutine. Cuz right now it is blocking this current thread
		// until the response is received. Instead, send the replicate request with a rereplicate ID and continue
		// doing other things. All writes (and ideally reads) for that file should be blocked. Then when we get 
		// a replicate_response, we can see if it was successful or not. If it was successful, we'll know which one
		// is done replicating based on the replicate ID, and then we can mark it as done and now that file is
		// ready to be read from and written to. Maybe store a hashmap of sdfs_filename -> replicateID currently in progress
		// so we know which one is done. Actually... we might not even need a rereplicate ID because we can use the 
		// sdfs_filename as the rereplicate ID, since it is guaranteed to be unique -- you're not gonna have 2 replication procedures
		// for the same file. If it was not successful, then we'll just recompute which nodes failed, which are available,
		// and then resend the rereplicatation request to those nodes.
		// ORR another solution would just be to call this 'notifyExistingReplicasToReReplicate()" function from a 
		// separate goroutine so that it doesn't block the current thread.
		response, response_err := ReceiveReReplicateResponse(replicaReader, true)   
		if response_err != nil {
			_ = replicaConn.Close()
			continue
		} else {
			if response.WasSuccessful {
				// so now update our namenode metadata information with the new nodes it has
				// the response should have the new nodes that it saved the file to - the new replicas
				leader.FileToNodes[sdfs_filename][0] = append(leader.FileToNodes[sdfs_filename][0], response.NewReplicas...)
				leader.FileOperations[sdfs_filename].CurrentlyProcessingFailedReplicasList = make([]core.NodeID, 0)
				if len(leader.FileOperations[sdfs_filename].FailedReplicasBuffer) == 0 {
					leader.FileOperations[sdfs_filename].NeedsReReplication = false
				}
				leader.FileOperations[sdfs_filename].StartedReReplicationProcedure = false // move back to false so another procedure can begin

				_ = replicaConn.Close()

				endTime := time.Now().UnixNano()
				elapsedTimeMs := (endTime - startTime) / 1000000
				fmt.Printf("Replication Elapsed Time: %d\n", elapsedTimeMs)

				break // done, we don't need to contact other nodes
			} else {
				_ = replicaConn.Close()
				continue
			}
		}
	}

}

func (leader *SDFSLeaderService) nodeExistsInNodeIDSlice(s []core.NodeID, elem core.NodeID) bool {
	for _, curr := range s {
		if curr == elem {
			return true
		}
	}
	return false
}

/*
Leader (this node) established a TCP connection with the client node, and
sends a *_INFO_RESPOSNE back to it. Either a
  - GET_INFO_RESPONSE
  - PUT_INFO_RESPONSE
  - DELETE_INFO_RESPONSE

depending on the file operation type (all this information is stored in the FileOperationTask pointer passed in)

This function does not lock any mutex locks. the caller is expected to lock
*/
func (leader *SDFSLeaderService) notifyClientToExecuteTask(task *FileOperationTask) {
	conn, err1 := net.Dial("tcp", task.ClientNode.IpAddress+":"+task.ClientNode.SDFSServerPort)
	if err1 != nil {
		fmt.Printf("Failed to Dial() client node: %s", err1)
		return
	}

	switch task.FileOpType {
	case GET_OP: // tell client which nodes the client should contact to get the data
		nodesToContact := leader.getNodesStoringFile(task.SdfsFilename)
		SendGetInfoResponse(conn, nodesToContact, task.SdfsFilename, task.ClientLocalFilename)

	case PUT_OP: // tell client which replica nodes to store the shards in
		var replicaNodes map[int][]core.NodeID
		var ok bool
		// if ok, then we already know which replicas have that file we wanna write to, otherwise we wanna choose new ones
		if replicaNodes, ok = leader.FileToNodes[task.SdfsFilename]; !ok {
			replicaNodes = leader.chooseReplicaNodesRandomly()
			leader.FileToNodes[task.SdfsFilename] = replicaNodes
		}
		SendPutInfoResponse(conn, replicaNodes, task.SdfsFilename, task.ClientLocalFilename)

	case DELETE_OP:
		nodesToContact := leader.getNodesStoringFile(task.SdfsFilename)
		SendDeleteInfoResponse(conn, nodesToContact, task.SdfsFilename)
	}

	err_connclose := conn.Close()
	if err_connclose != nil {
		log.Fatalln("Failed to close connection with client: ", err_connclose)
	}
}

/*
Helper function for the PUT operation. Given the file, it will determine the
replica nodes that the client should save the file too.
# It determines the replica nodes by choosing the 4 random nodes from its list of active nodes
Returns a map where

	key = shard index
	value = list of the replica nodes for that shard

Currently, we are not splitting up into shards so the map has only 1 key, which is equal to 0 for index 0
representing only 1 shard for the file

NOTE: currently we don't need to use a task *FileOperationTask because we are not sharding it, but if we do shard it
then we need to take in that as a parameter

NOTE: This function does not LOCK any mutex lock. It assumes that the caller of the function must lock anything necessary
*/
func (leader *SDFSLeaderService) chooseReplicaNodesRandomly() map[int][]core.NodeID {
	leader.shuffleActiveNodes()

	shardIdxToReplicas := make(map[int][]core.NodeID)
	replicaAmt := config.REPLICA_COUNT
	if len(leader.ActiveNodes) < replicaAmt {
		replicaAmt = len(leader.ActiveNodes)
	}
	replicaNodes := make([]core.NodeID, replicaAmt)
	copy(replicaNodes, leader.ActiveNodes[:replicaAmt])

	shardIdxToReplicas[0] = replicaNodes
	return shardIdxToReplicas
}

func (leader *SDFSLeaderService) shuffleActiveNodes() {
	// Fisher-Yates shuffle (https://yourbasic.org/golang/shuffle-slice-array/)
	for i := len(leader.ActiveNodes) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		leader.ActiveNodes[i], leader.ActiveNodes[j] = leader.ActiveNodes[j], leader.ActiveNodes[i]
	}
}

func (leader *SDFSLeaderService) createStringOfNodesForPutAck(nodes []core.NodeID) string {
	ret := "Put Stored in Replica Nodes:\n"
	for _, node := range nodes {
		ret += node.ToStringMP3() + "\n"
	}
	return ret
}

/*
Mark a task that was executing as completed

Returns true if it successfully found the task and finished it, otherwise it returns false
returns (bool, msg, additional info) to create an ack response
*/
func (leader *SDFSLeaderService) MarkTaskCompleted(clientId core.NodeID, sdfs_filename string) (bool, string, string, int64) {
	leader.MutexLock.Lock()
	md, ok := leader.FileOperations[sdfs_filename]
	if !ok {
		leader.MutexLock.Unlock()
		fmt.Println("Invalid sdfs_filename sent - could not mark task as completed")
		return false, "Invalid SDFS filename - does not exist", "", 0
	}

	// find which task it was based on the client that executed it

	// check write queue of current operations
	for i, writeTask := range md.CurrentWriteOps {
		if writeTask.ClientNode == clientId { // found!
			retString := writeTask.ToString() // return for additional information
			addInfo := ""
			startTime := writeTask.RequestedTime

			// get additional info for put
			if writeTask.FileOpType == PUT_OP {
				repNodes := leader.FileToNodes[writeTask.SdfsFilename][0]
				addInfo = leader.createStringOfNodesForPutAck(repNodes)
			}

			// delete index i when slice consists of pointers (https://github.com/golang/go/wiki/SliceTricks#delete-without-preserving-order)
			md.CurrentWriteOps[i] = md.CurrentWriteOps[len(md.CurrentWriteOps)-1]
			md.CurrentWriteOps[len(md.CurrentWriteOps)-1] = nil
			md.CurrentWriteOps = md.CurrentWriteOps[:len(md.CurrentWriteOps)-1]
			leader.MutexLock.Unlock()

			return true, retString, addInfo, startTime
		}
	}

	// check read queue of current operations
	for i, readTask := range md.CurrentReadOps {
		if readTask.ClientNode == clientId { // found!
			retString := readTask.ToString() // return for additional information
			startTime := readTask.RequestedTime

			// delete index i when slice consists of pointers (https://github.com/golang/go/wiki/SliceTricks#delete-without-preserving-order)
			md.CurrentReadOps[i] = md.CurrentReadOps[len(md.CurrentReadOps)-1]
			md.CurrentReadOps[len(md.CurrentReadOps)-1] = nil
			md.CurrentReadOps = md.CurrentReadOps[:len(md.CurrentReadOps)-1]
			leader.MutexLock.Unlock()
			return true, retString, "", startTime
		}
	}
	leader.MutexLock.Unlock()
	return false, "Leader did not find operation to mark as completed", "", 0
}

/*
Given a file operation, it will add it to its correct buffer so that the dispatcher can then schedule it
*/
func (leader *SDFSLeaderService) AddTask(fp *FileOperationTask) {
	leader.MutexLock.Lock()

	md, ok := leader.FileOperations[fp.SdfsFilename] // get file operation metadata of this file
	if !ok {
		// the sdfs_filename does not exist...
		if fp.FileOpType == GET_OP || fp.FileOpType == DELETE_OP {
			// TODO: handle error where the file does not exist...?
			// ^ ideally, we should send back a Negative ACK to the client - make this like a function to call
			fmt.Println("Invalid file does not exist")
			leader.MutexLock.Unlock()
			return
		} else if fp.FileOpType == PUT_OP {
			leader.addNewSDFSFile(fp.SdfsFilename)
			md = leader.FileOperations[fp.SdfsFilename]
		}
	}

	// queue this task to its respective buffer
	if fp.FileOpType == GET_OP {
		md.ReadBuffer = append(md.ReadBuffer, fp)
	} else if fp.FileOpType == PUT_OP || fp.FileOpType == DELETE_OP {
		md.WriteBuffer = append(md.WriteBuffer, fp)
	}

	leader.MutexLock.Unlock()
}

/*
This function does not lock any mutex locks. Caller is expected to lock any mutex locks
*/
func (leader *SDFSLeaderService) addNewSDFSFile(sdfs_filename string) {
	leader.FileOperations[sdfs_filename] = &FileOperationsMetadata{
		SdfsFilename:                          sdfs_filename,
		NeedsReReplication:                    false,
		StartedReReplicationProcedure:         false,
		FailedReplicasBuffer:                  make([]core.NodeID, 0),
		CurrentlyProcessingFailedReplicasList: make([]core.NodeID, 0),
		WriteBuffer:                           make([]*FileOperationTask, 0),
		ReadBuffer:                            make([]*FileOperationTask, 0),
		CurrentWriteOps:                       make([]*FileOperationTask, 0),
		CurrentReadOps:                        make([]*FileOperationTask, 0),
		NumConsecutiveWritesWithWaitingReads:  0,
		NumConsecutiveReadsWithWaitingWrites:  0,
	}
}

/*
Returns a list of NodeIDs of nodes that store a shard(s) of sdfs_filename
*/
func (leader *SDFSLeaderService) getNodesStoringFile(sdfs_filename string) []core.NodeID {
	nodes, ok := leader.FileToNodes[sdfs_filename]
	if !ok {
		log.Fatalln("Invalid sdfs_filename! Does not exist in SDFS")
	}

	return nodes[0] // TODO: change when sharding! Right now it just returns the replicas of the first shard
}

/*
Returns the result for a LS operation
*/
func (leader *SDFSLeaderService) LsOperation(sdfs_filename string) []core.NodeID {
	leader.MutexLock.Lock()
	defer leader.MutexLock.Unlock()
	return leader.getNodesStoringFile(sdfs_filename)
}

func (leader *SDFSLeaderService) PrefixMatchOperation(prefix string) []string {
	leader.MutexLock.Lock()
	defer leader.MutexLock.Unlock()

	matchingFilenames := make([]string, 0)

	for filename := range leader.FileToNodes { // iterate through all files in the distributed file system
		if strings.HasPrefix(filename, prefix) {
			matchingFilenames = append(matchingFilenames, filename)
		}
	}

	return matchingFilenames
}

//func (leader *SDFSLeaderService) AddFileToSDFS(sdfs_filename string, filesize int64) {
//	/* based on the filesize and the shard size, it will
//		1. Figure out how many shards will be required
//		2. For each shard, it will choose the R replica nodes that it should be assigned to
//	Return a list of NodeIDs, where the index of the list represents the shard index.
//	So list[1] will store all the replica NodeIDs that will store shard-1
//		list[6] will store all the replica NodeIDs that will store shard-6
//	*/
//	var num_shards int64
//	var last_shard_size int64
//	//num_shards := math.Ceil(float64(filesize) / config.SHARD_SIZE)
//	//filesize % config.SHARD_SIZE
//	num_shards = int64(math.Ceil(float64(filesize) / float64(config.SHARD_SIZE)))
//	last_shard_size = filesize % config.SHARD_SIZE
//	if last_shard_size == 0 { // evenly splits
//		last_shard_size = config.SHARD_SIZE
//	}
//
//	// index represents the shard_index
//	nodesPerShard := make([][]core.NodeID, num_shards)
//	var i int64
//	for i = 0; i < num_shards; i++ {
//		nodesPerShard[i] = make([]core.NodeID, 0)
//		// TODO: complete this function
//	}
//}

/*
When a node joins our group, add the node to the list of active nodes.
*/
func (leader *SDFSLeaderService) AddNewActiveNode(nodeId core.NodeID) {
	leader.MutexLock.Lock()
	leader.ActiveNodes = append(leader.ActiveNodes, nodeId)
	leader.MutexLock.Unlock()
}

/*
Indicate that a node id has failed.

Find which SDFS files were residing in that node and mark those SDFS files as needing to be re-replicated
in the file operations meta data. The dispatcher thread will then take care of actually initiating the
re-replicating process
*/
func (leader *SDFSLeaderService) IndicateNodeFailed(failed_nodeId core.NodeID) {
	leader.MutexLock.Lock()
	// double check that it actually removes...
	//fmt.Printf("Len(AvailableWorkerNodes) before Deletion = %d\n", len(leader.AvailableWorkerNodes))
	_ = leader.deleteActiveNode(failed_nodeId)
	//fmt.Printf("Len(AvailableWorkerNodes) after Deletion = %d\nAnd didDelete = %d\n", len(leader.AvailableWorkerNodes), didDelete)

	// find which SDFS files were affected by this node crash and
	for sdfs_filename, shardsToNodes := range leader.FileToNodes {
		for i, replicaNode := range shardsToNodes[0] { // TODO: only going through shard 1 because we assuming only 1 shard
			if replicaNode == failed_nodeId {
				// delete replicaNode from the list of replica nodes that this sdfs_filename has
				leader.FileToNodes[sdfs_filename][0][i] = leader.FileToNodes[sdfs_filename][0][len(leader.FileToNodes[sdfs_filename][0])-1]
				leader.FileToNodes[sdfs_filename][0] = leader.FileToNodes[sdfs_filename][0][:len(leader.FileToNodes[sdfs_filename][0])-1]

				// mark this file as needing re-replication
				leader.FileOperations[sdfs_filename].NeedsReReplication = true
				leader.FileOperations[sdfs_filename].FailedReplicasBuffer = append(leader.FileOperations[sdfs_filename].FailedReplicasBuffer, failed_nodeId)
				//msg := fmt.Sprintf("Marked %s file as needing re-replication for failed node replica (%s)", sdfs_filename, failed_nodeId.ToStringMP3())
				//logMessageHelper(leader.logFile, msg)

				//fmt.Printf("Len(leader.FileOperations[sdfs_filename].FailedReplicasBuffer) = %d\n", len(leader.FileOperations[sdfs_filename].FailedReplicasBuffer))
			}
		}
	}

	leader.MutexLock.Unlock()
}

func (leader *SDFSLeaderService) deleteActiveNode(nodeToDelete core.NodeID) bool {
	// first find it's index
	var i int
	var currNode core.NodeID
	finalI := -1

	for i, currNode = range leader.ActiveNodes {
		if currNode == nodeToDelete {
			finalI = i
			break
		}
	}

	if finalI != -1 { // found
		leader.ActiveNodes[finalI] = leader.ActiveNodes[len(leader.ActiveNodes)-1]
		leader.ActiveNodes = leader.ActiveNodes[:len(leader.ActiveNodes)-1]
		return true
	} else {
		return false
	}
}
