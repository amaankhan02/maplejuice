package maplejuice

import (
	"bufio"
	"cs425_mp4/internal/tcp_net"
	"cs425_mp4/internal/utils"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
)

type MapleJuiceNode struct {
	id            NodeID
	leaderID      NodeID
	isLeader      bool
	leaderService *MapleJuiceLeaderService
	tcpServer     *tcp_net.TCPServer
	logFile       *os.File
	sdfsNode      *SDFSNode
	workerTmpDir  string
	leaderTmpDir  string

	localWorkerTaskID int // just used internally by the worker to keep track of task number to create unique directories
}

/*
Parameters:

	thisId:
	leaderId:
	loggingFile:
	tmp_dir_name: name of the directory that this node should be saving files to
*/
func NewMapleJuiceNode(thisId NodeID, leaderId NodeID, loggingFile *os.File, sdfsNode *SDFSNode) *MapleJuiceNode {
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
func (this *MapleJuiceNode) executeMapleTask(
	numTasks int,
	exeFile string,
	exeFileArgs []string, // TODO: add compatibility to send this information from the client to leader to worker
	sdfsIntermediateFilenamePrefix string,
	sdfsSrcDirectory string,
	taskIndex int,
) {
	// load the input data and find our portion of the data
	// TODO: add mutex lock
	this.localWorkerTaskID++

	// TODO: delete these directories later - do a defer
	// create a temporary directory to store temporary files that this maple task needs to use
	// TODO: change this into a modular function - and test it
	tmp_task_dirname := filepath.Join(this.workerTmpDir, fmt.Sprintf("maple-%d-%d-%s", this.localWorkerTaskID, taskIndex, sdfsIntermediateFilenamePrefix))
	dataset_dir := filepath.Join(tmp_task_dirname, "dataset")
	output_kv_dir := filepath.Join(tmp_task_dirname, "output_kvs") // TODO: make these strings constants later
	dataset_dir_creation_err := os.MkdirAll(dataset_dir, 0744)
	if dataset_dir_creation_err != nil {
		log.Fatalln("Failed to create temporary dataset directory for maple task. Error: ", dataset_dir_creation_err)
	}
	output_dir_err := os.MkdirAll(output_kv_dir, 0744)
	if output_dir_err != nil {
		log.Fatalln("Failed to create temporary output directory for maple task. Error: ", dataset_dir_creation_err)
	}

	// get the dataset filenames & create corresponding local filenames to save it to
	sdfs_dataset_filenames := this.sdfsNode.PerformPrefixMatch(sdfsSrcDirectory + ".")
	local_dataset_filenames := make([]string, 0)
	for _, sdfs_dataset_filename := range sdfs_dataset_filenames {
		// TODO: make this a function or a constant formatter string above
		new_local_filename := filepath.Join(dataset_dir, "local-"+sdfs_dataset_filename)
		local_dataset_filenames = append(local_dataset_filenames, new_local_filename)
	}

	// retrieve the dataset files from distributed file system
	err := this.sdfsNode.PerformBlockedGets(sdfs_dataset_filenames, local_dataset_filenames)
	if err != nil {
		log.Fatalln("Error! Could not do PerformBlockedGets(): ", err)
	}

	// run the maple_exe's on all of them (sequentially or in parallel) & save them to one file
	/*
		Create an output file to save the key-value pairs to
		For every file,
			* count number of lines
			* based on this taskIndex, go to the line count that is assigned to this task
			* loop through the start and end of the task portion
				* read 100 lines, pipe into stdin for maple_exe
				* run maple_exe
				* pipe stdout out
				* loop through the stdout line by line and save it to the file

			* save it on separate files here based on keys...

			**NOTE: make this whole thing a separate function - and run test cases on it to ensure it works before sending it across...
	*/

	// inside tmp_kvpair_dir, we want to create a new file per key and store it in there, the name will be
	// sdfsIntermediate_filename_prefix_K where K is the key,
	for _, local_dataset_filename := range local_dataset_filenames {
		inputFile, open_input_err := os.OpenFile(local_dataset_filename, os.O_RDONLY, 0744)
		if open_input_err != nil {
			log.Fatalln("Failed to open local_dataset_file")
		}

		totalLines := utils.CountNumLinesInFile(inputFile)

		// calculate the start and end lines this task should handle
		linesPerTask := totalLines / int64(numTasks)
		remainder := totalLines % int64(numTasks)
		var numLinesToHandle int64

		if taskIndex == numTasks-1 && remainder != 0 { // handling last task and doesn't divide evenly
			numLinesToHandle = linesPerTask + remainder
		} else {
			numLinesToHandle = linesPerTask
		}

		startLine := int64(taskIndex) * linesPerTask
		endLine := startLine + numLinesToHandle
		currLine := startLine

		this.moveFilePointerToLine(currLine)
		// read config.LINES_PER_MAPLE_EXE at a time
		for currLine < endLine { // TODO: is this < or <=
			/*
				1. read line by line and pipe into stdin of the maple_exe - store it in a buffer and then pass in maybe
				2. execute maple_exe
				3. pipe out stdout into a buffer or read it line by line
					a) per line, open its respective kv file and append to it
					b) close that file
				4.
			*/
			// TODO: For SQL commands I also need to pass in the first line as well everytime
			cmd := exec.Command(exeFile)

			// TODO: IDEA - instead of piping in STDIN and STDOUT, just give the filename, startLine, and number of lines it should read
			// that way, it can handle the extra logic of if it wants the first line or not - makes it easy on my end
			// args: file, startline, numLinesToRead, additionalInfo
			// and the additional info will contain the column and regex information
		}
	}

	// send the task response back with the file data
}
