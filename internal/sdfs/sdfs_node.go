package sdfs

import (
	"bufio"
	"cs425_mp4/internal/config"
	"cs425_mp4/internal/core"
	"cs425_mp4/internal/tcp_net"
	"cs425_mp4/internal/utils"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

type LocalAck struct {
	ack           Ack
	elapsedTimeMs int64
}

/*
Currently implementing the following interfaces
  - FaultTolerable
  - tcp_net.TCPServerConnectionHandler
*/
type SDFSNode struct {
	id       core.NodeID
	leaderID core.NodeID
	IsLeader bool
	SdfsDir  string
	logFile  *os.File

	tcpServer         *tcp_net.TCPServer
	fileSystemService *FileSystemService
	LeaderService     *SDFSLeaderService

	// store a map of key = sdfs_filename+local_filename concatenated - just a unique way to ID a get operation
	// value is the wait group that we need to do a wg.Done() to ONLY IF IT EXISTS. if it DOES NOT EXIST, then no need to do it!
	blockedClientGets map[string]*sync.WaitGroup
	blockedClientPuts map[string]*sync.WaitGroup

	ackMutex   sync.Mutex
	clientAcks []LocalAck // store all Acks received to this client
}

func NewSDFSNode(thisId core.NodeID, introducerLeaderId core.NodeID, isIntroducerLeader bool, logFile *os.File, sdfsRootDir string) *SDFSNode {
	sdfsNode := &SDFSNode{
		id:                thisId,
		leaderID:          introducerLeaderId,
		IsLeader:          isIntroducerLeader,
		logFile:           logFile,
		SdfsDir:           sdfsRootDir,
		tcpServer:         nil,
		fileSystemService: nil,
		LeaderService:     nil, // leaderService is nil if this node is not the leader
		clientAcks:        make([]LocalAck, 0),
		blockedClientGets: make(map[string]*sync.WaitGroup),
		blockedClientPuts: make(map[string]*sync.WaitGroup),
	}
	sdfsNode.tcpServer = tcp_net.NewTCPServer(thisId.SDFSServerPort, sdfsNode)

	if isIntroducerLeader {
		fmt.Println("Initialized SDFSLeaderService")
		sdfsNode.LeaderService = NewSDFSLeaderService(config.T_DISPATCHER_WAIT,
			config.MAX_NUM_CONCURRENT_READS,
			config.MAX_NUM_CONCURRENT_WRITES,
			config.MAX_NUM_CONSECUTIVE_OPERATIONS,
			logFile,
		)
		sdfsNode.LeaderService.AddNewActiveNode(thisId) // add itself into the list of active nodes
	} else {
		fmt.Println("Initialized SDFSLeaderService to be NULL")
	}
	sdfsNode.fileSystemService = NewFileSystemService(sdfsRootDir)
	return sdfsNode
}

/*
Starts the node, which includes
  - Having the SDFSNode joining the group
*/
func (node *SDFSNode) Start() {
	node.tcpServer.StartServer()
	if node.LeaderService != nil {
		node.LeaderService.Start()
	}
	core.LogMessageln(os.Stdout, "SDFS Node has started")
	core.LogMessageln(node.logFile, "SDFS Node has started")
}

//func (node *SDFSNode) HandleNodeFailure(info FailureDetectionInfo) {
//	// only handle if we are the leader. cuz otherwise the gossip will eventually send it to the leader
//	if node.isLeader {
//		node.leaderService.IndicateNodeFailed(info.FailedNodeId)
//	}
//}
//
//func (node *SDFSNode) HandleNodeJoin(info NodeJoinInfo) {
//	// if a node joined our membership list, i need to reflect that in leaderService.AvailableWorkerNodes
//	if node.isLeader {
//		node.leaderService.AddNewActiveNode(info.JoinedNodeId)
//	}
//
//}

/*
Implementing TCPServerConnectionHandler interface

Called when a new TCP client connection is established with this TCP server.
Executed on a new goroutine.

Server-side handling a new client connection to this server port
Server handles:
  - Receiving REQUESTS
  - Sending RESPONSES

Steps:
  - read the message type (just 1 byte), based on that, I can know which type of message it is
    and then call the respective function to read that data in and then act accordingly.
*/
func (node *SDFSNode) HandleTCPServerConnection(conn net.Conn) {
	var msgType byte

	// Read Message Type
	reader := bufio.NewReader(conn)
	rawMessageType, _ := tcp_net.ReadMessageType(reader)

	msgType = rawMessageType
	dontCloseLeaderConn := false

	switch msgType {
	case GET_INFO_REQUEST:
		if !node.IsLeader {
			log.Fatalln("Non-leader received GET_INFO_REQUEST - Not allowed!!")
		}
		getInfoReq := ReceiveGetInfoRequest(reader)
		fp := &FileOperationTask{
			FileOpType:          GET_OP,
			SdfsFilename:        getInfoReq.SdfsFilename,
			ClientLocalFilename: getInfoReq.LocalFilename,
			ClientNode:          getInfoReq.ClientID,
			NewFileSize:         0, // set to 0 cuz GET operation doesn't use this value
			RequestedTime:       getInfoReq.Timestamp,
		}
		node.LeaderService.AddTask(fp)

	case PUT_INFO_REQUEST:
		if !node.IsLeader {
			log.Fatalln("Non-leader received PUT_INFO_REQUEST - Not allowed!!")
		}
		putInfoReq := ReceivePutInfoRequest(reader)
		fp := &FileOperationTask{
			FileOpType:          PUT_OP,
			SdfsFilename:        putInfoReq.SdfsFilename,
			ClientLocalFilename: putInfoReq.ClientLocalFilename,
			ClientNode:          putInfoReq.ClientID,
			NewFileSize:         putInfoReq.Filesize,
			RequestedTime:       putInfoReq.Timestamp,
		}
		node.LeaderService.AddTask(fp)

	case DELETE_INFO_REQUEST:
		if !node.IsLeader {
			log.Fatalln("Non-leader received DELETE_INFO_REQUEST - Not allowed!!")
		}

		delInfoReq := ReceiveDeleteInfoRequest(reader)
		fp := &FileOperationTask{
			FileOpType:    DELETE_OP,
			SdfsFilename:  delInfoReq.SdfsFilename,
			ClientNode:    delInfoReq.ClientID,
			RequestedTime: delInfoReq.Timestamp,
		}
		node.LeaderService.AddTask(fp)

	case LS_REQUEST:
		if !node.IsLeader {
			log.Fatalln("Non-leader received LS_REQUEST - Not allowed!!")
		}

		lsReq := ReceiveLsRequest(reader, false)
		replicas := node.LeaderService.LsOperation(lsReq.SdfsFilename)
		SendLsResponse(conn, lsReq.SdfsFilename, replicas)

	case PREFIX_MATCH_REQUEST:
		if !node.IsLeader {
			log.Fatalln("Non-leader received PREFIX_MATCH_REQUEST - Not allowed!!")
		}
		req := ReceivePrefixMatchRequest(reader, false)
		filenames := node.LeaderService.PrefixMatchOperation(req.SdfsFilenamePrefix)
		SendPrefixMatchResponse(conn, filenames)

	case ACK_RESPONSE: // leader receiving an ACK means a file operation was completed
		if !node.IsLeader {
			log.Fatalln("Non-leader received ACK_RESPONSE - Not allowed!\n Client will get ACK in an existing TCP connection not here")
		}
		ack_struct := ReceiveOnlyAckResponseData(reader)
		didFindTask, msg, addInfo, startTime := node.LeaderService.MarkTaskCompleted(ack_struct.SenderNodeId, ack_struct.AdditionalInfo)
		SendAckResponse(conn, node.id, didFindTask, msg, addInfo, startTime)

	case GET_DATA_REQUEST:
		getDataRequest := ReceiveGetDataRequest(reader)
		shards := node.fileSystemService.GetShards(getDataRequest.Filename)
		SendGetDataResponse(conn, shards)

	case PUT_DATA_REQUEST:
		putDataRequest := ReceivePutDataRequest(reader)
		node.fileSystemService.WriteShard(putDataRequest.Sharded_data)
		SendAckResponse(conn, node.id, true, "", "", 0)

	case DELETE_DATA_REQUEST:
		deleteDataRequest := ReceiveDeleteDataRequest(reader)
		node.fileSystemService.DeleteAllShards(deleteDataRequest.Filename) // delete all Shards locally w/ the sdfs_filename
		SendAckResponse(conn, node.id, true, "", "", 0)

	case REREPLICATE_REQUEST:
		rr_req := ReceiveReReplicateRequest(reader, false)
		node.performReReplicate(conn, rr_req)
		dontCloseLeaderConn = true

	case GET_INFO_RESPONSE:
		node.clientHandleReceiveGetInfoResponse(conn, reader)
		dontCloseLeaderConn = true

	case PUT_INFO_RESPONSE:
		node.clientHandleReceivePutInfoResponse(conn, reader)
		dontCloseLeaderConn = true

	case DELETE_INFO_RESPONSE:
		node.clientHandleReceiveDeleteInfoResponse(conn, reader)
		dontCloseLeaderConn = true

	case MULTIREAD_REQUEST:
		mr_req := ReceiveMultiReadRequest(reader, false)
		node.PerformGet(mr_req.SdfsFilename, mr_req.LocalFilename)

	default:
		log.Fatalf("Received invalid message type for leader (type: %02x)\n", msgType)
	}

	if !dontCloseLeaderConn {
		close_err := conn.Close()
		if close_err != nil {
			log.Fatalln("Failed to close connection in Server Connection Handler: ", close_err)
		}
	}
}

func (node *SDFSNode) performReReplicate(leaderConn net.Conn, rr_req *ReReplicateRequest) {
	// contact the replica nodes with a PUT_DATA_REQUEST operation on the SDFS_FILENAME
	// based on the success/failure, we notify leaderConn with a ReReplicateReponse
	currentNewReplicasAccomplished := 0
	newFinalReplicas := make([]core.NodeID, 0)

	for _, nodeToContact := range rr_req.NewPossibleReplicaNodes {
		nodeConn, err2 := net.Dial("tcp", nodeToContact.IpAddress+":"+nodeToContact.SDFSServerPort)
		nodeConnReader := bufio.NewReader(nodeConn)

		if err2 != nil {
			fmt.Println("Failed to Dial to potential replica node server. Going to try another node")
			continue
		}
		localFilename := filepath.Join(node.SdfsDir, rr_req.SdfsFilename)
		shard := node.createSingleShardFromFile(localFilename, rr_req.SdfsFilename)
		SendPutDataRequest(nodeConn, shard)
		ack_resp := ReceiveFullAckResponse(nodeConnReader) // TODO: if this fails it log.Fatals() - might be potential bug! instead return an err

		if ack_resp.WasSuccessful { // TODO: change these to logging
			currentNewReplicasAccomplished += 1
			newFinalReplicas = append(newFinalReplicas, nodeToContact)
		}
		// else {
		//   logMessageHelper(node.logFile, "Received ACK from replica - failed to put! Will try another node...")
		// }

		_ = nodeConn.Close()
		if currentNewReplicasAccomplished == rr_req.NumNewReplicasRequired {
			SendReReplicateResponse(leaderConn, rr_req.SdfsFilename, newFinalReplicas, true)
			return
		}
	}
	SendReReplicateResponse(leaderConn, rr_req.SdfsFilename, newFinalReplicas, false)

	_ = leaderConn.Close()
}

func (node *SDFSNode) printLsResponse(response *LsResponse) {
	fmt.Printf("Nodes with SDFS_FileName %s:\n", response.SdfsFilename)
	for _, replicaNode := range response.Replicas {
		fmt.Println("\t" + replicaNode.ToStringMP3())
	}
	fmt.Println()
}

func (node *SDFSNode) clientHandleReceiveGetInfoResponse(leaderConn1 net.Conn, leaderReader1 *bufio.Reader) {
	getInfoResponse := ReceiveGetInfoResponse(leaderReader1) // contains list of nodes to contact
	leaderConn1.Close()
	nodesToContact := getInfoResponse.NodeIds

	// hashset of shards written. key = shard index. Used so that we don't write the same shard twice
	shardsWritten := make(map[int]struct{})
	shardsToWrite := make(map[int]Shard)

	// serially contact all the nodes, get the data, and write to local file
	// NOTE: write now this only works where the file has just one shard - demo purposes this is fine
	// NOTE: but when the shards are split among many nodes this has to be changed
	for _, nodeId := range nodesToContact { // when num shards = 1, then the nodes to contact are all replicas
		nodeConn, err2 := net.Dial("tcp", nodeId.IpAddress+":"+nodeId.SDFSServerPort)
		nodeConnReader := bufio.NewReader(nodeConn)
		if err2 != nil {
			fmt.Println("Failed to Dial to replica node server. Going to try another replica")
			continue
		}

		SendGetDataRequest(nodeConn, getInfoResponse.SdfsFilename)

		nodeConnReadMsgType, err32 := tcp_net.ReadMessageType(nodeConnReader)
		if err32 != nil {
			log.Fatalln("Failed to read message type for GET DATA RESPONSE inside clientHandleReceiveGetInfoResponse - ", err32)
		} else if nodeConnReadMsgType != GET_DATA_RESPONSE {
			log.Fatalln("Message Type read was is != GET_DATA_RESPONSE --- bug!")
		}

		getDataResponse := ReceiveGetDataResponse(nodeConnReader) // you'll get back just 1 shard - which is the entire file
		var totalFileSize int64 = 0

		for _, shard := range getDataResponse.Shards {
			if _, ok := shardsWritten[shard.Metadata.ShardIndex]; ok {
				continue // i already previously wrote this shard
			}
			totalFileSize += shard.Metadata.Size

			shardsToWrite[shard.Metadata.ShardIndex] = shard
			shardsWritten[shard.Metadata.ShardIndex] = struct{}{} // record the shard index to not use this again
		}

		_ = nodeConn.Close()

		// check if we got all the shards. if we did, then we don't need to contact the other replicas
		if len(shardsWritten) == getDataResponse.TotalNumberOfShardsInActualSDFSFile {
			break
		}
	}
	WriteShardsToLocalFile(getInfoResponse.ClientLocalFilename, shardsToWrite)

	// contact leader again
	leaderConn2, err2 := net.Dial("tcp", node.leaderID.IpAddress+":"+node.leaderID.SDFSServerPort)
	if err2 != nil {
		fmt.Println("Failed to establish TCP connection to leader! - ", err2)
		return
	}
	leaderReader2 := bufio.NewReader(leaderConn2)

	SendAckResponse(leaderConn2, node.id, true, "GET operation successfully executed", getInfoResponse.SdfsFilename, 0)
	ack_response := ReceiveFullAckResponse(leaderReader2)
	localAck := LocalAck{
		ack:           *ack_response,
		elapsedTimeMs: (time.Now().UnixNano() - ack_response.StartTime) / 1000000,
	}
	node.printAck(&localAck, false)
	node.addAcknowledgement(&localAck)

	// check if this was a blocking get operation - if so, then call wg.Done() to indicate it's done
	wg, key_exists := node.blockedClientGets[getInfoResponse.SdfsFilename+getInfoResponse.ClientLocalFilename]
	if key_exists {
		wg.Done()
	}

	errL := leaderConn2.Close()
	if errL != nil {
		log.Fatalln("Failed to close connection to leader: ", errL)
	}

}

func (node *SDFSNode) printAck(localAck *LocalAck, printToStdout bool) {
	msg := fmt.Sprintf("Received ACK for Completed Task:\n%s\n%s\nElapsed Time (ms): %d\n",
		localAck.ack.Message, localAck.ack.AdditionalInfo, localAck.elapsedTimeMs)
	if printToStdout {
		fmt.Println(msg)
	} else {
		_, _ = fmt.Fprintln(node.logFile, msg)
	}
}

func (node *SDFSNode) addAcknowledgement(localAck *LocalAck) {
	node.ackMutex.Lock()
	node.clientAcks = append(node.clientAcks, *localAck)
	node.ackMutex.Unlock()
}

func (node *SDFSNode) PerformAcknowledgementsPrint() {
	node.ackMutex.Lock()
	fmt.Println("Printing all Acknowledgements")
	for i, ack := range node.clientAcks {
		fmt.Printf("%d) ", i+1)
		node.printAck(&ack, true)
	}
	node.ackMutex.Unlock()
}

func (node *SDFSNode) clientHandleReceivePutInfoResponse(leaderConn1 net.Conn, leaderReader1 *bufio.Reader) {
	// Get response back from leader -
	putInfoResponse := ReceivePutInfoResponse(leaderReader1)
	leaderConn1.Close()

	replicaNodes := putInfoResponse.Shard_num_to_machines_list[0] // just access shard 0 since only 1 shard

	// TODO: just creating single shard for now - when wanting to create multiple, use respective helper functions already created below
	shard := node.createSingleShardFromFile(putInfoResponse.ClientLocalFilename, putInfoResponse.SdfsFilename)
	isSuccess := true
	for _, machine := range replicaNodes {
		// dial replica node
		replicaConn, err1 := net.Dial("tcp", machine.IpAddress+":"+machine.SDFSServerPort)
		replicaConnReader := bufio.NewReader(replicaConn)
		if err1 != nil {
			fmt.Println("Failed to Dial to replica server. Error: ", err1)
			continue
		}

		SendPutDataRequest(replicaConn, shard) // TODO: add logging
		ReceiveFullAckResponse(replicaConnReader)

		_ = replicaConn.Close()
	}

	// now we need to create a new connection with the leader
	leaderConn2, err2 := net.Dial("tcp", node.leaderID.IpAddress+":"+node.leaderID.SDFSServerPort)
	if err2 != nil {
		fmt.Println("Failed to establish TCP connection to leader! - ", err2)
		return
	}
	leaderReader2 := bufio.NewReader(leaderConn2)

	// let leader know PUT operation is completed
	SendAckResponse(leaderConn2, node.id, isSuccess, "", putInfoResponse.SdfsFilename, 0)
	leader_ack := ReceiveFullAckResponse(leaderReader2)

	localAck := LocalAck{
		ack:           *leader_ack,
		elapsedTimeMs: (time.Now().UnixNano() - leader_ack.StartTime) / 1000000,
	}

	node.printAck(&localAck, false)
	node.addAcknowledgement(&localAck)

	// check if this was a blocking get operation - if so, then call wg.Done() to indicate it's done
	wg, key_exists := node.blockedClientPuts[putInfoResponse.ClientLocalFilename+putInfoResponse.SdfsFilename]
	if key_exists {
		wg.Done()
	}

	errL := leaderConn2.Close()
	if errL != nil {
		log.Fatalln("Failed to close connection to leader: ", errL)
	}
}

func (node *SDFSNode) clientHandleReceiveDeleteInfoResponse(leaderConn1 net.Conn, leaderReader1 *bufio.Reader) {
	// Get response back from leader -
	delInfoResponse := ReceiveDeleteInfoResponse(leaderReader1)
	leaderConn1.Close()

	replicaNodes := delInfoResponse.Replicas // just access shard 0 since only 1 shard

	for _, machine := range replicaNodes {
		// dial replica node
		replicaConn, err1 := net.Dial("tcp", machine.IpAddress+":"+machine.SDFSServerPort)
		replicaConnReader := bufio.NewReader(replicaConn)
		if err1 != nil {
			fmt.Println("Failed to Dial to replica server. Error: ", err1)
			continue
		}

		SendDeleteDataRequest(replicaConn, delInfoResponse.SdfsFilename) // TODO: add logging
		ReceiveFullAckResponse(replicaConnReader)
		_ = replicaConn.Close()
	}

	// now we need to create a new connection with the leader
	leaderConn2, err2 := net.Dial("tcp", node.leaderID.IpAddress+":"+node.leaderID.SDFSServerPort)
	if err2 != nil {
		fmt.Println("Failed to establish TCP connection to leader! - ", err2)
		return
	}
	leaderReader2 := bufio.NewReader(leaderConn2)

	// let leader know PUT operation is completed
	SendAckResponse(leaderConn2, node.id, true, "", delInfoResponse.SdfsFilename, 0)
	leader_ack := ReceiveFullAckResponse(leaderReader2)

	localAck := LocalAck{
		ack:           *leader_ack,
		elapsedTimeMs: (time.Now().UnixNano() - leader_ack.StartTime) / 1000000,
	}

	node.printAck(&localAck, false)
	node.addAcknowledgement(&localAck)

	errL := leaderConn2.Close()
	if errL != nil {
		log.Fatalln("Failed to close connection to leader: ", errL)
	}
}

func WriteShardsToLocalFile(localFilename string, shards map[int]Shard) {
	file, err1 := os.OpenFile(localFilename, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755)
	if err1 != nil {
		log.Fatalf("Failed to open file %s\n", localFilename)
	}

	for shard_idx, shard := range shards {
		_, err2 := file.Write(shard.Data)
		if err2 != nil {
			log.Fatalf("Failed to write shard data. Shard Index: %d", shard_idx)
		}
	}

	err := file.Close()
	if err != nil {
		log.Fatalln("Failed to close file")
	}
}

/*
Performs the GET file operation. It retrieves the file with the name 'sdfsfilename'
from the distributed file system, and writes that file to the local Filename 'localfilename'

Below are the steps. For the networking operations like "send get info request", it uses the corresponding
functions from 'sdfs_network.go'

Steps:
 0. Create TCP Client socket and connect to the leader node
 1. Send GetInfo Request to leader node
 2. Close connection to leader.

Later, when the leader schedules this GET operation, it will send a GET_INFO_RESPONSE back to this node,
which is read and handled in the TCP Server connection handler function.
*/
func (node *SDFSNode) PerformGet(sdfsfilename string, localFilename string) {
	leaderConn, err1 := net.Dial("tcp", node.leaderID.IpAddress+":"+node.leaderID.SDFSServerPort)
	if err1 != nil {
		fmt.Println("Failed to Dial to leader server. Error: ", err1)
		return
	}

	SendGetInfoRequest(leaderConn, sdfsfilename, localFilename, node.id)
	//logMessageHelper(node.logFile, "Sent GET INFO REQUEST to leader")
	e2 := leaderConn.Close()
	if e2 != nil {
		log.Fatalln("Couldn't close Leader connection: ", e2)
	}
}

func (node *SDFSNode) PerformBlockedGets(sdfs_filenames []string, local_filenames []string) error {
	if len(sdfs_filenames) != len(local_filenames) {
		return errors.New("sdfs_filenames has different length than local_filenames")
	}

	var wg sync.WaitGroup

	for i := 0; i < len(sdfs_filenames); i++ {
		wg.Add(1)

		// TODO: ideally the key should be the ID of the GET so that its gauranteed to be unique. change this later in future
		// right now its just the sdfs_filename + local_filename
		node.blockedClientGets[sdfs_filenames[i]+local_filenames[i]] = &wg
		node.PerformGet(sdfs_filenames[i], local_filenames[i])
	}

	wg.Wait() // block here

	// just delete all the key value pairs created from this performblockedget()
	for i := 0; i < len(sdfs_filenames); i++ {
		delete(node.blockedClientGets, sdfs_filenames[i]+local_filenames[i])
	}

	return nil
}

func (sdfsNode *SDFSNode) PerformBlockedPuts(local_filenames []string, sdfs_filenames []string) error {
	if len(sdfs_filenames) != len(local_filenames) {
		return errors.New("sdfs_filenames has different length than local_filenames")
	}

	var wg sync.WaitGroup

	for i := 0; i < len(sdfs_filenames); i++ {
		wg.Add(1)

		// TODO: ideally the key should be the ID of the PUT so that its gauranteed to be unique. change this later in future
		sdfsNode.blockedClientPuts[local_filenames[i]+sdfs_filenames[i]] = &wg
		sdfsNode.PerformPut(local_filenames[i], sdfs_filenames[i])
	}

	wg.Wait() // block here

	// just delete all the key value pairs created from this performblockedput() since its done
	for i := 0; i < len(sdfs_filenames); i++ {
		delete(sdfsNode.blockedClientPuts, local_filenames[i]+sdfs_filenames[i])
	}

	return nil
}

/*
Performs the PUT file operation.

Below are the steps. For the networking operations like "send get info request", it uses the corresponding
functions from 'sdfs_network.go'

Steps:
 1. Send Put info request to leader node
    a) the PutInfoRequest should have the sdfsfilename but also metadata information about the file, like
    its filesize - anything for the leader to determine how to make the Shards and such...
 2. The leader will then later send back a PutInfoResponse which we will handle in the tcp server connection
    handler function
*/
func (node *SDFSNode) PerformPut(localfilename string, sdfsfilename string) {
	// create connection with leader
	leaderConn, err1 := net.Dial("tcp", node.leaderID.IpAddress+":"+node.leaderID.SDFSServerPort)
	if err1 != nil {
		fmt.Println("Failed to Dial to leader server. Error: ", err1)
		return
	}
	defer leaderConn.Close()

	file_size := utils.GetFileSize(localfilename)

	SendPutInfoRequest(leaderConn, sdfsfilename, localfilename, file_size, node.id)
}

// Pretty similar to GET/PUT except that we do a DELETE operation.
func (node *SDFSNode) PerformDelete(sdfsfilename string) {
	// create connection with leader
	leaderConn, err1 := net.Dial("tcp", node.leaderID.IpAddress+":"+node.leaderID.SDFSServerPort)
	if err1 != nil {
		fmt.Println("Failed to Dial to leader server. Error: ", err1)
		return
	}
	defer leaderConn.Close()

	SendDeleteInfoRequest(leaderConn, sdfsfilename, node.id)
}

/*
List all machine addresses where this file is currently being stored
 1. contact leader
 2. ask leader where files are currently being stored
 3. because leader knows filename -> machines
    use GET
    send back info response
 4. use GET
 5. print out all machines
 5. do not need any data requests/responses
*/
func (node *SDFSNode) PerformLs(sdfs_file_name string) {
	leaderConn, err1 := net.Dial("tcp", node.leaderID.IpAddress+":"+node.leaderID.SDFSServerPort)
	if err1 != nil {
		fmt.Println("Failed to Dial to leader server. Error: ", err1)
		return
	}
	defer leaderConn.Close()
	reader := bufio.NewReader(leaderConn)

	SendLsRequest(leaderConn, sdfs_file_name)
	//logMessageHelper(node.logFile, "Sent LS REQUEST")

	lsResp := ReceiveLsResponse(reader, true)
	//logMessageHelper(node.logFile, "Received LS RESPONSE")
	node.printLsResponse(lsResp)
}

/*
Requests the leader to return all filenames that match a certain prefix in the sdfs_filename
*/
func (node *SDFSNode) PerformPrefixMatch(sdfs_filename_prefix string) []string {
	leaderConn, err1 := net.Dial("tcp", node.leaderID.IpAddress+":"+node.leaderID.SDFSServerPort)
	if err1 != nil {
		fmt.Println("Failed to Dial to leader server. Error: ", err1)
		return nil
	}
	defer leaderConn.Close()
	reader := bufio.NewReader(leaderConn)
	SendPrefixMatchRequest(leaderConn, sdfs_filename_prefix)

	response := ReceivePrefixMatchResponse(reader, true)
	return response.SdfsFilenames
}

/*
Print out all files currnently being stored at this machine
*/
func (node *SDFSNode) PerformStore() {
	file_to_shards := node.fileSystemService.ShardMetadatas

	fmt.Println("All SDFS Filenames stored locally:")
	for file := range file_to_shards {
		fmt.Println("\t" + file)
	}
	fmt.Println()
}

func (node *SDFSNode) PerformMultiRead(sdfsFilename string, localFilename string, VMs []string) {
	targetNodes := make([]core.NodeID, 0)
	for _, vmStr := range VMs {
		vm_num, err := strconv.ParseInt(vmStr[2:], 10, 64)
		if err != nil {
			fmt.Printf("Failed to parse %s. Invalid multiread request\n", vmStr)
			return
		}
		hostname := utils.GetHostname(int(vm_num))
		ip := utils.GetIP(hostname)
		sdfs_port := utils.GetSDFSPort(int(vm_num))
		targetNodes = append(targetNodes, core.NodeID{
			IpAddress:      ip,
			GossipPort:     "",
			SDFSServerPort: sdfs_port,
			Timestamp:      0,
			Hostname:       hostname,
		})
	}

	// now contact them
	for _, targetNode := range targetNodes {
		// TODO: you could make this block of code to send as a goroutine in an annonymous function to have it truly "simultaneous"
		targetConn, err2 := net.Dial("tcp", targetNode.IpAddress+":"+targetNode.SDFSServerPort)
		if err2 != nil {
			fmt.Printf("Failed to establish TCP connection with %s. Not sending them a multiread!\n", targetNode.Hostname)
			continue
		}

		//logMessageHelper(node.logFile, "Going to send multi read request")
		SendMultiReadRequest(targetConn, sdfsFilename, localFilename)
		//logMessageHelper(node.logFile, fmt.Sprintf("Sent MULTIREAD REQUEST to %s", targetNode.Hostname))
		_ = targetConn.Close()
	}
	//logMessageHelper(node.logFile, "Finished sending to all requested VMs")
}

// --------------- HELPER FUNCTIONS ------------------

/*
Gives you what each shard num's shard struct is. Because this is what you send over to the datanodes so they store it
Given:
  - maplejuice filename
  - map, key = shard num, val = list of machines to send shards to
  - total file size
  - used to calculate the size of the last shard (which is the only one with different size shard)

Return
  - map, key = shard num, val = shard struct
*/
func SplitShards(sdfsfilename string, shard_num_to_machines_list map[int][]core.NodeID, total_file_size int64) map[int]Shard {

	shard_num_to_shard_struct := make(map[int]Shard)

	shard_num_to_data := GetShardNumToShardData(sdfsfilename)
	num_shards := len(shard_num_to_machines_list)

	for shard_num := range shard_num_to_machines_list {

		// if last shard - size of shard can be arbitrary, otherwise it is the fixed shard_size
		var shard_size int64
		shard_size = 0

		if shard_num == num_shards-1 {
			shard_size = total_file_size - (config.SHARD_SIZE * (int64(num_shards) - 1))
		} else {
			shard_size = config.SHARD_SIZE
		}

		// create shard metadata
		shard_file_name := fmt.Sprintf(config.SHARD_LOCAL_FILENAME_FORMAT, shard_num, sdfsfilename)
		shard_meta_data := ShardMetaData{
			shard_num,
			sdfsfilename,
			shard_file_name,
			shard_size,
		}

		shard := Shard{
			shard_num_to_data[shard_num],
			shard_meta_data,
		}

		shard_num_to_shard_struct[shard_num] = shard
	}

	return shard_num_to_shard_struct
}

/*
Get Actual Data (in bytes) of shard, for each shard num
Given:
  - local file

Return:
  - Map, key = shard num, val = actual shard data
*/
func GetShardNumToShardData(local_file_path string) map[int][]byte {
	shard_num_to_data := make(map[int][]byte)

	// open file
	file, err := os.Open(local_file_path)
	if err != nil {
		fmt.Println("Error in opening file: ", err)
		return nil
	}
	defer file.Close()

	// create buffer - will be fixed size every time
	buffer := make([]byte, config.SHARD_SIZE)
	shard_num := 0

	for {
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error in reading file: ", err)
			return nil
		}

		// have to create a copy so you don't accidently overwrite data
		shard := make([]byte, n)
		copy(shard, buffer[:n])

		shard_num_to_data[shard_num] = shard

		shard_num += 1
	}

	return shard_num_to_data
}

func (node *SDFSNode) createSingleShardFromFile(localfilename string, sdfsFilename string) *Shard {
	filesize := utils.GetFileSize(localfilename)
	buff := make([]byte, filesize)

	file, err1 := os.OpenFile(localfilename, os.O_RDONLY, 0777)
	if err1 != nil {
		log.Fatalf("Could not open file %s", localfilename)
	}
	defer file.Close()

	_, err2 := file.Read(buff)
	if err2 != nil {
		log.Fatalf("Could not read file %s", localfilename)
	}

	shardMd := ShardMetaData{
		ShardIndex:    0,
		SdfsFilename:  sdfsFilename,
		ShardFilename: sdfsFilename, // TODO: since just single shard we keep it as same filename, otherwise must use the SHARD FORMAT CONFIG VAR
		Size:          filesize,
	}

	shard := &Shard{
		Data:     buff,
		Metadata: shardMd,
	}

	return shard
}

// /*
// Leader picks machines to send shards to
// Given:
//   - total file size
//   - num_replicas (const)
//   - shard_size (const)
//
// Want every shard to be the same size but last shard can be any arbitrary size
// Return:
//   - Map: key = shard num, value = list of NodeIds to send DataBlock (all replicas) to
//     -
//
// */
// func (node *SDFSNode) PickMachinesToSendDataBlock(file_size byte) []core.NodeID {
//
//		// find all other machines to choose from
//		membership_list := node.ThisGossipNode.MembershipList.MemList
//
//		// round robbin through membership list
//		// keep track of which position in membership list you are at
//		round_robin_index := 0
//
//		machines_to_send_datablocks := make(map[int][]core.NodeID)
//		num_shards := int(file_size / SHARD_SIZE) + 1
//
//		// for each shard num, assign some machines
//		for shard_num := 1; shard_num <= num_shards; shard_num++ {
//
//			//machines := make([]core.NodeID, 0)
//
//			if round_robin_index+NUM_REPLICAS > len(membership_list) {
//
//
//			}
//			else {
//
//				// go from
//				for mem_list_index := round_robin_index; mem_list_index < mem_list_index + NUM_REPLICAS; mem_list_index++ {
//
//				}
//			}
//
//			machines := membership_list[round_robin_index : round_robin_index+NUM_REPLICAS]
//
//			// update around robin index
//			round_robin_index = (round_robin_index + NUM_REPLICAS) % len(membership_list)
//
//			machines_to_send_datablocks[shard_num] = machines
//
//			// start at 0 and get next 4 machines
//			// from that index get the next four machines
//			// look through membership list
//
//		}
//
//		// TESTING
//		fmt.Println()
//		fmt.Println("Machines to send datablocks to: ")
//		for _, id := range machines_to_send_datablocks {
//			fmt.Println(id)
//		}
//		return machines_to_send_datablocks
//	}
// func logMessageHelper(logStream *os.File, message string) {
// 	core.LogMessageln(os.Stdout, message)
// 	if logStream != nil {
// 		core.LogMessageln(logStream, message)
// 	}
// }
