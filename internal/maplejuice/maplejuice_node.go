package maplejuice

import (
	"bufio"
	"cs425_mp4/internal/tcp_net"
	"fmt"
	"net"
	"os"
)

type MapleJuiceNode struct {
	id            NodeID
	leaderID      NodeID
	isLeader      bool
	leaderService *MapleJuiceLeaderService
	tcpServer     *tcp_net.TCPServer
	logFile       *os.File
	sdfsNode      *SDFSNode
}

/*
Parameters:

	thisId:
	leaderId:
	loggingFile:
	tmp_dir_name: name of the directory that this node should be saving files to
*/
func NewMapleJuiceNode(thisId NodeID, leaderId NodeID, loggingFile *os.File, task_output_dir string, sdfsNode *SDFSNode) *MapleJuiceNode {
	mj := &MapleJuiceNode{
		id:       thisId,
		leaderID: leaderId,
		isLeader: leaderId == thisId,
		logFile:  loggingFile,
		sdfsNode: sdfsNode,
	}
	mj.tcpServer = tcp_net.NewTCPServer(thisId.MapleJuiceServerPort, mj)
	if mj.isLeader {
		fmt.Println("Initialized MapleJuiceLeaderService")
		// TODO: pass task_output_dir to the the leader service
		mj.leaderService = NewMapleJuiceLeaderService()
	} else {
		fmt.Println("Initialized MapleJuiceLeaderService to be NULL")
		mj.leaderService = nil
	}

	return mj
}

func (this *MapleJuiceNode) HandleTCPServerConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	mjNetworkMessage, recv_err := ReceiveMJNetworkMessage(reader)
	if recv_err != nil {
		this.logBoth(fmt.Sprintf("Error in ReceiveMJNetworkMessage: %s\n", recv_err))
		return
	}
	if this.isLeader {
		switch mjNetworkMessage.MsgType {
		case MAPLE_JOB_REQUEST:
			this.leaderService.SubmitMapleJob(
				mjNetworkMessage.ExeFile,
				mjNetworkMessage.NumTasks,
				mjNetworkMessage.SdfsIntermediateFilenamePrefix,
				mjNetworkMessage.SdfsSrcDirectory,
			)
		case JUICE_JOB_REQUEST:
			this.leaderService.SubmitJuiceJob(
				mjNetworkMessage.ExeFile,
				mjNetworkMessage.NumTasks,
				mjNetworkMessage.SdfsIntermediateFilenamePrefix,
				mjNetworkMessage.SdfsDestFilename,
				mjNetworkMessage.ShouldDeleteJuiceInput,
				mjNetworkMessage.JuicePartitionScheme,
			)
		case MAPLE_TASK_RESPONSE:
			this.leaderService.ReceiveMapleTaskOutput(
				conn,
				mjNetworkMessage.CurrTaskIdx,
				mjNetworkMessage.TaskOutputFileSize,
				this.sdfsNode,
			)
		case JUICE_TASK_RESPONSE:
			panic("not implemented")
			// TODO: can the leader act as a client to submit a job request?
		}

	} else {
		switch mjNetworkMessage.MsgType {
		case MAPLE_TASK_REQUEST: // must execute some task and send back to leader
			// you don't know how long this will take... so run this on a separate go routine and then send back the
			// data later
			this.executeMapleTask(
				mjNetworkMessage.NumTasks,
				mjNetworkMessage.ExeFile,
				mjNetworkMessage.SdfsIntermediateFilenamePrefix,
				mjNetworkMessage.SdfsSrcDirectory,
				mjNetworkMessage.CurrTaskIdx,
			)
		case JUICE_TASK_REQUEST: // must execute some task and send back to leader
			panic("not implemented")
		case MAPLE_JOB_RESPONSE: // acknowledging the job is done
			panic("not implemented")
		case JUICE_JOB_RESPONSE: // acknowledging the job is done
			panic("not implemented")

		}
	}

	_ = conn.Close()
}

func (this *MapleJuiceNode) Start() {
	this.tcpServer.StartServer()
	if this.isLeader {
		this.leaderService.Start()
	}
	this.logBoth("Maple Juice Node has started!")
}

/*
Executes a Maple phase given the input parameters. Submits a Maple Job to the leader
and the leader takes care of scheduling the job. Leader later responds back with
an acknowledgement
*/
func (this *MapleJuiceNode) PerformMaple(maple_exe string, num_maples int, sdfs_intermediate_filename_prefix string,
	sdfs_src_directory string) {

	mjJob := &MapleJuiceNetworkMessage{
		MsgType:                        MAPLE_JOB_REQUEST,
		NumTasks:                       num_maples,
		ExeFile:                        maple_exe,
		SdfsIntermediateFilenamePrefix: sdfs_intermediate_filename_prefix,
		SdfsSrcDirectory:               sdfs_src_directory,
		ClientId:                       this.id,
	}
	leaderConn, err := net.Dial("tcp", this.leaderID.IpAddress+":"+this.leaderID.SDFSServerPort)
	if err != nil {
		fmt.Println("Failed to Dial to leader server. Error: ", err)
		return
	}
	defer leaderConn.Close()

	SendMJNetworkMessage(leaderConn, mjJob) // submit job to leader
}

func (this *MapleJuiceNode) PerformJuice(juice_exe string, num_juices int, sdfs_intermediate_filename_prefix string,
	sdfs_dest_filename string, shouldDeleteInput bool, partitionScheme JuicePartitionType) {

	mjJob := &MapleJuiceNetworkMessage{
		MsgType:                        JUICE_JOB_REQUEST,
		JuicePartitionScheme:           partitionScheme,
		NumTasks:                       num_juices,
		ExeFile:                        juice_exe,
		SdfsIntermediateFilenamePrefix: sdfs_intermediate_filename_prefix,
		SdfsDestFilename:               sdfs_dest_filename,
		ShouldDeleteJuiceInput:         shouldDeleteInput,
		ClientId:                       this.id,
	}

	leaderConn, err := net.Dial("tcp", this.leaderID.IpAddress+":"+this.leaderID.SDFSServerPort)
	if err != nil {
		fmt.Println("Failed to Dial to leader server. Error: ", err)
		return
	}
	defer leaderConn.Close()

	SendMJNetworkMessage(leaderConn, mjJob)
}

// --------------------------------------------------------------------
func (this *MapleJuiceNode) logBoth(msg string) {
	LogMessageln(os.Stdout, msg)
	LogMessageln(this.logFile, msg)
}

/*
Ideally this should run on a separate go routine because this may take some time to run...

# Executes the maple task assigned to this worker node

Every file int he sdfsSrcDirectory gets split up by the number of mapper tasks, and the current task id
is used to determine split we are assigned to run for this file. We repeat this on every file.
and we run the exe file on 20 lines at a time.

TODO:
  - Ideally, we should sort the outputs based on the key, and store it in separate files here, and then send
    multiple files. in the MJNetworkMessage struct I can attach like the number of files its sending and the key
    that they file belongs to as parallel lists. and then on the leader side we can just merge the sorted
    stuff together so that its faster. and that way we won't have to read everything into memory either
    cuz we can just do one line at a time.
*/
func (this *MapleJuiceNode) executeMapleTask(numTasks int, exeFile string, sdfsIntermediateFilenamePrefix string, sdfsSrcDirectory string, taskIndex int) {
	// load the input data and find our portion of the data
	//dataset_files := this.sdfsNode.PerformPrefixMatch(sdfsSrcDirectory + ".")
	//local_dataset_files :=
	//this.sdfsNode.PerformBlockedGets(dataset_files, )
	/*
		sdfs.PerfomGet(sdfs_directory, sdfs_filename, localfilepath)
		sdfs.PerformPut(sdfs_directory, sdfs_filename, localfilepath)

		leader (PUT)
			if directory == "":
				then store as sdfs_filename
			else:
				store as "sdfs_directory.sdfs_filename" (i.e., delimit w/ a period)
		leader (GET):
			if directory == "":
				store as sdfs_filename
			else:
				store as "sdfs_directory.sdfs_filename" (i.e, delimit w/ a period)
	*/

	// run the maple_exe's on all of them (sequentially or in parallel) & save them to one file

	// send the task response back with the file data
}
