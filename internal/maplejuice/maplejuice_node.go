package maplejuice

import (
	"bufio"
	"cs425_mp4/internal/config"
	"cs425_mp4/internal/tcp_net"
	"cs425_mp4/internal/utils"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

const MAPLE_TASK_DIR_NAME_FMT = "maple-%d-%d-%s" // formats: this.localWorkerTaskID, taskIndex, sdfsIntermediateFilenamePrefix
const MAPLE_TASK_DATASET_DIR_NAME = "dataset"
const MAPLE_TASK_OUTPUT_FILENAME = "maple_task_output.csv"
const LOCAL_SDFS_DATASET_FILENAME_FMT = "local-%s" // when you GET the sdfs_filename, this is the localfilename you want to save it as

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
	exeColumnSchema string, // TODO: wrap these 3 exe parameters into a struct - this is an optional
	exeAdditionalInfo string, // TODO: add compatibility to send this information from the client to leader to worker
	sdfsIntermediateFilenamePrefix string,
	sdfsSrcDirectory string,
	taskIndex int,
) {
	// TODO: add mutex lock
	this.localWorkerTaskID++

	maple_task_dirpath, dataset_dirpath, maple_task_output_file :=
		this.createTempDirsAndFilesForMapleTask(taskIndex, sdfsIntermediateFilenamePrefix)

	// get the dataset filenames & create corresponding local filenames to save it to
	sdfs_dataset_filenames := this.sdfsNode.PerformPrefixMatch(sdfsSrcDirectory + ".")
	local_dataset_filenames := make([]string, 0)
	for _, curr_sdfs_dataset_filename := range sdfs_dataset_filenames {
		// TODO: make this a function or a constant formatter string above
		new_local_filename := filepath.Join(dataset_dirpath, fmt.Sprintf(LOCAL_SDFS_DATASET_FILENAME_FMT, curr_sdfs_dataset_filename))
		local_dataset_filenames = append(local_dataset_filenames, new_local_filename)
	}

	// retrieve the dataset files from distributed file system
	if get_err := this.sdfsNode.PerformBlockedGets(sdfs_dataset_filenames, local_dataset_filenames); get_err != nil {
		log.Fatalln("Error! Could not do PerformBlockedGets(): ", get_err)
	}

	// for each dataset file, find the portion this task runs on, execute maple exe on it for how many
	// ever times it needs to finish the entire portion, and output the outputs for maple exe to output_kv_file
	for _, local_dataset_filename := range local_dataset_filenames {
		inputFile, open_input_err := os.OpenFile(local_dataset_filename, os.O_RDONLY, 0744)
		if open_input_err != nil {
			log.Fatalln("Failed to open local_dataset_file")
		}

		totalLines := utils.CountNumLinesInFile(inputFile)
		startLine, endLine := this.calculateStartAndEndLinesForTask(totalLines, numTasks, taskIndex)
		utils.MoveFilePointerToLineNumber(inputFile, startLine)
		var numLinesForExe int64

		// increment currLine by the amount of lines that it read
		for currLine := startLine; currLine < endLine; currLine += numLinesForExe {
			numLinesForExe = min(endLine-currLine, int64(config.LINES_PER_MAPLE_EXE))
			exe_args := []string{strconv.FormatInt(numLinesForExe, 10), exeColumnSchema, exeAdditionalInfo}
			this.executeMapleExe(exeFile, exe_args, inputFile, maple_task_output_file, numLinesForExe)
		}

		_ = inputFile.Close()
	}

	// send the task response back with the file data

	// close and delete the temporary files & dirs
	_ = maple_task_output_file.Close()
	if delete_tmp_dir_err := utils.DeleteDirAndAllContents(maple_task_dirpath); delete_tmp_dir_err != nil {
		log.Fatalln("Failed to delete maple_task_dirpath and all its contents. Error: ", delete_tmp_dir_err)
	}
}

/*
Creates temporary directory and files for a maple task inside the directory given for this MapleJuiceNode

/task_dirname								(CREATED HERE)

	|- /dataset_dirname  					(CREATED HERE)
		|- dataset files pulled from sdfs 	(NOT CREATED HERE)
	|- maple_task_output_file				(CREATED HERE)
*/
func (this *MapleJuiceNode) createTempDirsAndFilesForMapleTask(taskIndex int, sdfsIntermediateFilenamePrefix string) (string, string, *os.File) {
	task_dirpath := filepath.Join(this.workerTmpDir, fmt.Sprintf(MAPLE_TASK_DIR_NAME_FMT, this.localWorkerTaskID, taskIndex, sdfsIntermediateFilenamePrefix))
	dataset_dirpath := filepath.Join(task_dirpath, MAPLE_TASK_DATASET_DIR_NAME)
	output_kv_filepath := filepath.Join(task_dirpath, MAPLE_TASK_OUTPUT_FILENAME)

	if dataset_dir_creation_err := os.MkdirAll(dataset_dirpath, 0744); dataset_dir_creation_err != nil {
		log.Fatalln("Failed to create temporary dataset directory for maple task. Error: ", dataset_dir_creation_err)
	}
	maple_task_output_file, output_file_open_err := os.OpenFile(output_kv_filepath, os.O_CREATE|os.O_APPEND, 0744)
	if output_file_open_err != nil {
		log.Fatalln("Failed to create temporary output file for maple task. Error: ", output_file_open_err)
	}

	return task_dirpath, dataset_dirpath, maple_task_output_file
}

/*
	calculateStartAndEndLinesForTask

Helper function used to determine the start and end lines that the current maple task
should operate on. Given the entire file, we need to know which portion that this task
is assigned to. This function figures that out.

Parameters:

	totalLines (int64): number of total lines in the file
	numTasks (int): number of total tasks that this map/juice job is doing
	taskIndex (int): 0-indexed number representing the task that this is working on. Based on this number,
					it will determine the start and end lines

Returns

	startLine (int64): 1-indexed number representing the line number we should start reading from
	endLine (int64): 1-index number representing the line number to stop at - exclusive. Meaning, we should NOT read the
				line with line number 'endLine'.
*/
func (this *MapleJuiceNode) calculateStartAndEndLinesForTask(totalLines int64, numTasks int, taskIndex int) (int64, int64) {
	linesPerTask := totalLines / int64(numTasks)
	remainder := totalLines % int64(numTasks)
	var numLinesTaskShouldHandle int64

	if taskIndex == numTasks-1 && remainder != 0 { // handling last task and doesn't divide evenly
		numLinesTaskShouldHandle = linesPerTask + remainder
	} else {
		numLinesTaskShouldHandle = linesPerTask
	}

	startLine := int64(taskIndex)*linesPerTask + 1  // + 1 so that its 1-indexed
	endLine := startLine + numLinesTaskShouldHandle // exclusive

	return startLine, endLine
}

/*
Executes the maple exe.

Reads inputFile line by line, and feeds it into stdin pipe for maple_exe shell command.
It also reads the stdout line by line and writes it to outputFile, line by line
*/
func (this *MapleJuiceNode) executeMapleExe(
	maple_exe string,
	args []string,
	inputFile *os.File,
	outputFile *os.File,
	numLinesToProcess int64,
) {
	cmd := exec.Command(maple_exe, args...)

	stdin_pipe, in_pipe_err := cmd.StdinPipe()
	if in_pipe_err != nil {
		panic(in_pipe_err)
	}
	stdout_pipe, out_pipe_err := cmd.StdoutPipe()
	if out_pipe_err != nil {
		panic(out_pipe_err)
	}

	// start command but don't block
	if start_err := cmd.Start(); start_err != nil {
		panic(start_err)
	}

	// read from input file, and write line by line to stdin pipe
	inputFileScanner := bufio.NewScanner(inputFile)
	stdinInPipeWriter := bufio.NewWriter(stdin_pipe)
	for linesRead := int64(0); linesRead < numLinesToProcess; linesRead++ {
		if inputFileScanner.Scan() == false { // if = false, then got an EOF
			panic("Failed to read a line from input file! This shouldn't happen!")
		}
		line := inputFileScanner.Text() + "\n" // Scan() does not contain the new line character
		_, write_err := stdinInPipeWriter.WriteString(line)
		if write_err != nil {
			panic(write_err)
		}
	}
	_ = stdinInPipeWriter.Flush() // make sure everything was written to it
	if in_pipe_close_err := stdin_pipe.Close(); in_pipe_close_err != nil {
		panic(in_pipe_close_err)
	}

	// read stdout
	// TODO: will io.ReadAll() work? Because maple_exe doesn't output an EOF... will this block forever? test this out
	stdout_bytes, read_stdout_err := io.ReadAll(stdout_pipe)
	if read_stdout_err != nil {
		panic(read_stdout_err)
	}

	// write stdout to output file
	output_writer := bufio.NewWriter(outputFile)
	_, output_write_err := output_writer.Write(stdout_bytes)
	if output_write_err != nil {
		panic(output_write_err)
	}

	// wait for program to finish
	if wait_err := cmd.Wait(); wait_err != nil {
		panic(wait_err)
	}
}
