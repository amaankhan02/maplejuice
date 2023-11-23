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

func NewMapleJuiceNode(thisId NodeID, leaderId NodeID, loggingFile *os.File) *MapleJuiceNode {
	mj := &MapleJuiceNode{
		id:       thisId,
		leaderID: leaderId,
		isLeader: leaderId == thisId,
		logFile:  loggingFile,
	}
	mj.tcpServer = tcp_net.NewTCPServer(thisId.MapleJuiceServerPort, mj)
	if mj.isLeader {
		fmt.Println("Initialized MapleJuiceLeaderService")
		mj.leaderService = NewMJLeaderService()
	} else {
		fmt.Println("Initialized MapleJuiceLeaderService to be NULL")
		mj.leaderService = nil
	}

	return mj
}

func (this *MapleJuiceNode) HandleTCPServerConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)

	if this.isLeader {
		mjNetworkMessage, recv_err := ReceiveMJNetworkMessage(reader)
		if recv_err != nil {
			this.logBoth(fmt.Sprintf("Error in ReceiveMJNetworkMessage: %s\n", recv_err))
			return
		}

		switch mjNetworkMessage.MsgType {
		case MAPLE_JOB_REQUEST:

		case JUICE_JOB_REQUEST:
			panic("not implemented")
		case MAPLE_TASK_RESPONSE:
			panic("not implemented")
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
