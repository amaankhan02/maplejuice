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
}

/*
Parameters:

	thisId:
	leaderId:
	loggingFile:
	tmp_dir_name: name of the directory that this node should be saving files to
*/
func NewMapleJuiceNode(thisId NodeID, leaderId NodeID, loggingFile *os.File, task_output_dir string) *MapleJuiceNode {
	mj := &MapleJuiceNode{
		id:       thisId,
		leaderID: leaderId,
		isLeader: leaderId == thisId,
		logFile:  loggingFile,
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
			// receive that a task has finished... could we get a response that a task has failed though?
			// maybe... but we won't handle that. only time we'll assume a task failed is if the node entirely failed

			// read the file they sent with it which contains the
			/*
				read the file they sent with it which contains the key value pairs
				this method should actually probably be in one of the maplejuice_network.go functions to handle all this
				I should probably have MapleJuiceNode create a local tmp directory that contains any temporary files
				so that we can store it in there.
				and to make sure duplicates are avoided, we can maybe attach a job id (maybe have leader service store a
				job id that keeps getting incremented? - maybe not needed though)

				and after all task responses have been received, the leader service can then go in and read those files,
				parse them, and create the corresponding SDFS files and save them.

				we can call a function here to read the output into a local file that's in the maple juice node's local tmp dir.
				and then when we call leaderService.recordtaskresopnseoutput(), we can send the saved filepath for the output
				and we can give the corresponding task id.
			*/

			//tcp_net.ReadFile(somelocalfilename, conn, mjNetworkMessage.TaskOutputFileSize)
			//this.leaderService.ReceiveTaskResponseOutput(conn, mjNetworkMessage.CurrTaskIdx, mjNetworkMessage.TaskOutputFileSize)
			//this.leaderService.RecordFinishedTask(mjNetworkMessage.CurrTaskIdx)	// task index is how leader will know which task finished

			/*
				Task Response
					* task index (id)
					* file with key/value pairs for that particular task
					*
			*/
		case JUICE_TASK_RESPONSE:
			panic("not implemented")
			// TODO: can the leader act as a client to submit a job request?
		}

	} else {

	}
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
