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
	"sync"
	"time"
)

/*
Used ONLY by the client for its own bookkeeping
*/
type ClientMapleJuiceJob struct {
	ClientJobId int
	JobType     MapleJuiceJobType
}

type MapleJuiceNode struct {
	id            NodeID
	leaderID      NodeID
	isLeader      bool
	leaderService *MapleJuiceLeaderService
	tcpServer     *tcp_net.TCPServer
	logFile       *os.File
	sdfsNode      *SDFSNode
	nodeTmpDir    string // temporary directory used by this node to store temporary files for maple/juice tasks & leader service

	localWorkerTaskID int // just used internally by the worker to keep track of task number to create unique directories

	// map of clientJobId to ClientMapleJuiceJob representing current jobs that have been submitted from this client and waiting to be finished
	currentClientJobs        map[int]*ClientMapleJuiceJob
	totalClientJobsSubmitted int // used for generating unique job ids
}

type MapleJuiceExeFile struct {
	ExeFilePath string

	// Maple exe additional args (other than the number of lines that's passed in)
	ExeColumnSchema   string
	ExeAdditionalInfo string
}

const LEADER_TMP_DIR = "leader"

const MAPLE_TASK_DIR_NAME_FMT = "mapletask-%d-%d-%s" // formats: this.localWorkerTaskID, taskIndex, sdfsIntermediateFilenamePrefix
const MAPLE_TASK_DATASET_DIR_NAME = "dataset"
const MAPLE_TASK_OUTPUT_FILENAME = "maple_task_output.csv"

const JUICE_TASK_DIR_NAME_FMT = "juicetask-%d"                // formats: this.localWorkerTaskID -- we dont rlly need any other info for the dir name the id is guaranteed to be unique
const JUICE_LOCAL_INPUT_SDFS_INTERM_FILENAME_FMT = "local-%s" // formats: sdfsIntermediateFilenamePrefix
const JUICE_TASK_OUTPUT_FILENAME = "juice_task_output.csv"

const LOCAL_SDFS_DATASET_FILENAME_FMT = "local-%s" // when you GET the sdfs_filename, this is the localfilename you want to save it as
const JOB_DONE_MSG_FMT = "%s Job with ClientJobID %d has completed!\n"

func NewMapleJuiceNode(thisId NodeID, leaderId NodeID, loggingFile *os.File, sdfsNode *SDFSNode,
	mapleJuiceTmpDir string, leaderServiceDispatcherWaitTime time.Duration) *MapleJuiceNode {
	mj := &MapleJuiceNode{
		id:                       thisId,
		leaderID:                 leaderId,
		isLeader:                 leaderId == thisId,
		logFile:                  loggingFile,
		sdfsNode:                 sdfsNode,
		nodeTmpDir:               mapleJuiceTmpDir,
		localWorkerTaskID:        0,
		currentClientJobs:        make(map[int]*ClientMapleJuiceJob),
		totalClientJobsSubmitted: 0,
	}

	mj.tcpServer = tcp_net.NewTCPServer(thisId.MapleJuiceServerPort, mj)
	if mj.isLeader {
		fmt.Println("Initialized MapleJuiceLeaderService")
		leaderTmpDir := filepath.Join(mapleJuiceTmpDir, LEADER_TMP_DIR)
		mj.leaderService = NewMapleJuiceLeaderService(leaderId, leaderServiceDispatcherWaitTime, loggingFile, leaderTmpDir)
	} else {
		fmt.Println("Initialized MapleJuiceLeaderService to be NULL")
		mj.leaderService = nil
	}

	return mj
}

func (mjNode *MapleJuiceNode) Start() {
	mjNode.tcpServer.StartServer()
	if mjNode.isLeader {
		mjNode.leaderService.Start()
	}
	mjNode.logBoth("Maple Juice Node has started!")
}

func (this *MapleJuiceNode) HandleTCPServerConnection(conn net.Conn) {
	reader := bufio.NewReader(conn)
	mjNetworkMessage, recv_err := ReceiveMJNetworkMessage(reader)
	alreadyClosedLeaderConn := false

	if recv_err != nil {
		this.logBoth(fmt.Sprintf("Error in ReceiveMJNetworkMessage: %s\n", recv_err))
		return
	}
	// You don't know how some of these executions may take. So we close the conn object in the switch case
	// immediately once we don't need it anymore. Or one of the functions it calls may close it

	if this.isLeader { // LEADER NODE
		switch mjNetworkMessage.MsgType {
		case MAPLE_JOB_REQUEST:
			_ = conn.Close()
			alreadyClosedLeaderConn = true

			this.leaderService.SubmitMapleJob(
				mjNetworkMessage.ExeFile,
				mjNetworkMessage.NumTasks,
				mjNetworkMessage.SdfsIntermediateFilenamePrefix,
				mjNetworkMessage.SdfsSrcDirectory,
				mjNetworkMessage.ClientJobId,
			)
		case JUICE_JOB_REQUEST:
			_ = conn.Close()
			alreadyClosedLeaderConn = true

			this.leaderService.SubmitJuiceJob(
				mjNetworkMessage.ExeFile,
				mjNetworkMessage.NumTasks,
				mjNetworkMessage.SdfsIntermediateFilenamePrefix,
				mjNetworkMessage.SdfsDestFilename,
				mjNetworkMessage.ShouldDeleteJuiceInput,
				mjNetworkMessage.JuicePartitionScheme,
				mjNetworkMessage.ClientJobId,
			)
		case MAPLE_TASK_RESPONSE:
			// this function will read from the connection to get the file and then close the connection
			this.leaderService.ReceiveMapleTaskOutput(
				conn,
				mjNetworkMessage.CurrTaskIdx,
				mjNetworkMessage.TaskOutputFileSize,
				this.sdfsNode,
			)
			alreadyClosedLeaderConn = true
		case JUICE_TASK_RESPONSE:
			panic("not implemented yet")

		}
	} else { // NOT LEADER NODE
		switch mjNetworkMessage.MsgType {
		case MAPLE_TASK_REQUEST: // must execute some task and send back to leader
			_ = conn.Close()
			alreadyClosedLeaderConn = true

			this.executeMapleTask(
				mjNetworkMessage.NumTasks,
				mjNetworkMessage.ExeFile,
				mjNetworkMessage.SdfsIntermediateFilenamePrefix,
				mjNetworkMessage.SdfsSrcDirectory,
				mjNetworkMessage.CurrTaskIdx,
			)
		case JUICE_TASK_REQUEST: // must execute some task and send back to leader
			_ = conn.Close()
			alreadyClosedLeaderConn = true

			this.executeJuiceTask(
				mjNetworkMessage.ExeFile,
				mjNetworkMessage.SdfsIntermediateFilenamePrefix,
				mjNetworkMessage.Keys,
			)

		case MAPLE_JOB_RESPONSE: // acknowledging the job is done
			this.handleJobResponse(mjNetworkMessage.ClientJobId)
		case JUICE_JOB_RESPONSE: // acknowledging the job is done
			this.handleJobResponse(mjNetworkMessage.ClientJobId)
		}
	}

	if !alreadyClosedLeaderConn {
		_ = conn.Close()
	}
}

/*
Executes a Maple phase given the input parameters. Submits a Maple Job to the leader
and the leader takes care of scheduling the job. Leader later responds back with
an acknowledgement
*/
func (this *MapleJuiceNode) SubmitMapleJob(maple_exe MapleJuiceExeFile, num_maples int, sdfs_intermediate_filename_prefix string,
	sdfs_src_directory string) {

	// TODO: mutex lock
	clientJob := &ClientMapleJuiceJob{
		ClientJobId: this.totalClientJobsSubmitted,
		JobType:     MAPLE_JOB,
	}

	this.totalClientJobsSubmitted++
	this.currentClientJobs[clientJob.ClientJobId] = clientJob // store it for later... until we recieve the ACK
	// TODO: mutex unlock

	this.logBoth(fmt.Sprintf("Submitting Maple Job with ClientJobID %d to leader\n", clientJob.ClientJobId))

	mjJob := &MapleJuiceNetworkMessage{
		MsgType:                        MAPLE_JOB_REQUEST,
		NumTasks:                       num_maples,
		ExeFile:                        maple_exe,
		SdfsIntermediateFilenamePrefix: sdfs_intermediate_filename_prefix,
		SdfsSrcDirectory:               sdfs_src_directory,
		ClientId:                       this.id,
		ClientJobId:                    clientJob.ClientJobId,
	}
	leaderConn, err := net.Dial("tcp", this.leaderID.IpAddress+":"+this.leaderID.SDFSServerPort)
	if err != nil {
		fmt.Println("Failed to Dial to leader server. Unable to submit job request. Error: ", err)
		return
	}
	defer leaderConn.Close()

	SendMapleJuiceNetworkMessage(leaderConn, mjJob) // submit job to leader
}

func (this *MapleJuiceNode) SubmitJuiceJob(juice_exe MapleJuiceExeFile, num_juices int, sdfs_intermediate_filename_prefix string,
	sdfs_dest_filename string, shouldDeleteInput bool, partitionScheme JuicePartitionType) {

	// TODO: mutex lock
	clientJob := &ClientMapleJuiceJob{
		ClientJobId: this.totalClientJobsSubmitted,
		JobType:     JUICE_JOB,
	}

	this.totalClientJobsSubmitted++
	this.currentClientJobs[clientJob.ClientJobId] = clientJob // store it for later... until we recieve the ACK
	// TODO: mutex unlock

	this.logBoth(fmt.Sprintf("Submitting Juice Job with ClientJobID %d to leader\n", clientJob.ClientJobId))

	mjJob := &MapleJuiceNetworkMessage{
		MsgType:                        JUICE_JOB_REQUEST,
		JuicePartitionScheme:           partitionScheme,
		NumTasks:                       num_juices,
		ExeFile:                        juice_exe,
		SdfsIntermediateFilenamePrefix: sdfs_intermediate_filename_prefix,
		SdfsDestFilename:               sdfs_dest_filename,
		ShouldDeleteJuiceInput:         shouldDeleteInput,
		ClientId:                       this.id,
		ClientJobId:                    clientJob.ClientJobId,
	}

	leaderConn, err := net.Dial("tcp", this.leaderID.IpAddress+":"+this.leaderID.SDFSServerPort)
	if err != nil {
		fmt.Println("Failed to Dial to leader server. Error: ", err)
		return
	}
	defer leaderConn.Close()

	SendMapleJuiceNetworkMessage(leaderConn, mjJob)
}

// --------------------------------------------------------------------

/*
Handle when the client receives a job response from the leader notifying that it has finished
*/
func (this *MapleJuiceNode) handleJobResponse(clientJobId int) {
	// TODO: mutex lock
	jobTypeString := "Maple"
	if this.currentClientJobs[clientJobId].JobType == JUICE_JOB {
		jobTypeString = "Juice"
	}

	fmt.Printf(JOB_DONE_MSG_FMT, jobTypeString, clientJobId)

	delete(this.currentClientJobs, clientJobId) // remove the clientJobId from the map since its finished
	// TODO: mutex unlock -- for clientJobs[]
}

func (this *MapleJuiceNode) logBoth(msg string) {
	LogMessageln(os.Stdout, msg)
	LogMessageln(this.logFile, msg)
}

/*
Handles the juice tasks assigned to this worker node. Each key is another juice task.

Each juice task will output one key,value pair. So for all the juice tasks, we can save their outputs to just one file.

Handle each key in parallel in a separate go routine to make it faster.
For each key, grab the one intermediate file that has the key, and run the juice exe on it. Read in from stdin
and pass it the input file, and read the stdout which should be one key value pair. Return that through a channel.
On the outside, we can run a select{} to wait for all the channels to return, and then write the key value pairs
to the output file.
Once all channels have been read from, we can send the output file back to the leader.
*/
func (this *MapleJuiceNode) executeJuiceTask(juiceExe MapleJuiceExeFile, sdfsIntermediateFilenamePrefix string, assignedKeys []string) {

	// TODO: add mutex lock
	this.localWorkerTaskID++
	localWorkerTaskId := this.localWorkerTaskID
	// TODO: mutex unlock

	taskDirPath, juiceTaskOutputFile := this.createTempDirsAndFilesForJuiceTask(localWorkerTaskId)

	// get the names of the files to fetch from sdfs
	sdfsInputFilenames := this.createSdfsFilenamesFromIntermediateAndKeys(sdfsIntermediateFilenamePrefix, assignedKeys)

	// create the names of the corresponding local filenames
	localInputFilenames := make([]string, 0)
	for _, sdfsInputFilename := range sdfsInputFilenames {
		localInputFilenames = append(localInputFilenames, filepath.Join(taskDirPath, fmt.Sprintf(JUICE_LOCAL_INPUT_SDFS_INTERM_FILENAME_FMT, sdfsInputFilename)))
	}

	// fetch the files from sdfs to local tmp dir
	this.sdfsNode.PerformBlockedGets(sdfsInputFilenames, localInputFilenames)

	var wg sync.WaitGroup
	juiceExeOutputsChan := make(chan string, len(assignedKeys)) // buffered channel so that we don't block on the go routines

	// start a goroutine to execute each juice exe
	for i, _ := range assignedKeys {
		wg.Add(1)
		go this.executeJuiceExeOnKey(juiceExe.ExeFilePath, localInputFilenames[i], juiceExeOutputsChan)
		// each task will generate just one key-value pair, which will be returned on the channel
	}

	// close the channel once all goroutines have finished - do it in separate goroutine so that we don't block
	go func() {
		wg.Wait()
		// we must close otherwise the for-loop below where we read from the channel will block forever cuz it will read as long as the channel is open
		close(juiceExeOutputsChan)
	}()

	for result := range juiceExeOutputsChan {
		juiceTaskOutputFile.WriteString(result) // TODO: ensure that the result has the \n char at the end
	}

	_ = juiceTaskOutputFile.Close() // close file since we are done writing to it.

	// send the task response back with the file data to the leader
	leaderConn, conn_err := net.Dial("tcp", this.leaderID.IpAddress+":"+this.leaderID.MapleJuiceServerPort)
	if conn_err != nil {
		log.Fatalln("Failed to dial to leader server. Error: ", conn_err)
	}
	SendJuiceTaskResponse(leaderConn, juiceTaskOutputFile.Name(), assignedKeys) // ? any other information we gotta send back?
	_ = leaderConn.Close()
}

/*
Execute juice_exe on the inputFilepath, and write the output to the outputChan
Juice Exe will execute on the entire input file.

This function pipes the input file to the stdin of the juice_exe, and reads the stdout of the juice_exe
and returns that output through the outputChan

Parameters:

	juice_exe (string): name of the juice exe file, exactly as you would type it in the terminal
	inputFilepath (string): path to the input file that the juice exe will read from
	outputChan (chan string): channel to write the output of the juice exe to. This function will write to this channel

TODO: must test this function
*/
func (this *MapleJuiceNode) executeJuiceExeOnKey(juice_exe string, inputFilepath string, outputChan chan string) {
	cmd := exec.Command(juice_exe)

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
	inputFile, open_input_err := os.OpenFile(inputFilepath, os.O_RDONLY, 0744)
	if open_input_err != nil {
		log.Fatalln("Failed to open input file")
	}

	inputFileScanner := bufio.NewScanner(inputFile)
	stdinInPipeWriter := bufio.NewWriter(stdin_pipe)
	for inputFileScanner.Scan() {
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
	stdout_bytes, read_stdout_err := io.ReadAll(stdout_pipe)
	if read_stdout_err != nil {
		panic(read_stdout_err)
	}

	// wait for program to finish
	if wait_err := cmd.Wait(); wait_err != nil {
		panic(wait_err)
	}

	// write stdout to channel
	stdout_string := string(stdout_bytes) // TODO: test if this has the \n char in it or not
	if stdout_string[len(stdout_string)-1] != '\n' {
		stdout_string += "\n"
	}
	outputChan <- string(stdout_bytes)
}

func (this *MapleJuiceNode) createSdfsFilenamesFromIntermediateAndKeys(sdfsIntermediateFilenamePrefix string, assignedKeys []string) []string {
	sdfsFilenames := make([]string, 0)
	for _, key := range assignedKeys {
		// use the same function to generate the filename that the leader uses to generate the filename when storing it
		sdfsFilenames = append(sdfsFilenames, getSdfsIntermediateFilename(sdfsIntermediateFilenamePrefix, key))
	}
	return sdfsFilenames
}

/*
Ideally this should run on a separate go routine because this may take some time to run...

# Executes the maple task assigned to this worker node

Every file int he sdfsSrcDirectory gets split up by the number of mapper tasks, and the current task id
is used to determine split we are assigned to run for this file. We repeat this on every file.
and we run the exe file on 20 lines at a time.
*/
func (this *MapleJuiceNode) executeMapleTask(
	numTasks int,
	mapleExe MapleJuiceExeFile,
	sdfsIntermediateFilenamePrefix string,
	sdfsSrcDirectory string,
	taskIndex int,
) {
	// TODO: add mutex lock
	this.localWorkerTaskID++
	localWorkerTaskId := this.localWorkerTaskID

	maple_task_dirpath, dataset_dirpath, maple_task_output_file :=
		this.createTempDirsAndFilesForMapleTask(taskIndex, sdfsIntermediateFilenamePrefix, localWorkerTaskId)

	// get the dataset filenames & create corresponding local filenames to save it to
	sdfs_dataset_filenames := this.sdfsNode.PerformPrefixMatch(sdfsSrcDirectory + ".")
	local_dataset_filenames := make([]string, 0)
	for _, curr_sdfs_dataset_filename := range sdfs_dataset_filenames {
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
			exe_args := []string{strconv.FormatInt(numLinesForExe, 10), mapleExe.ExeColumnSchema, mapleExe.ExeAdditionalInfo}
			this.executeMapleExe(mapleExe.ExeFilePath, exe_args, inputFile, maple_task_output_file, numLinesForExe)
		}

		_ = inputFile.Close()
	}
	_ = maple_task_output_file.Close() // close file since we are done writing to it.

	// send the task response back with the file data
	leaderConn, conn_err := net.Dial("tcp", this.leaderID.IpAddress+":"+this.leaderID.MapleJuiceServerPort)
	if conn_err != nil {
		log.Fatalln("Failed to dial to leader server. Error: ", conn_err)
	}
	defer leaderConn.Close()

	SendMapleTaskResponse(leaderConn, taskIndex, maple_task_output_file.Name())

	// close and delete the temporary files & dirs
	if delete_tmp_dir_err := utils.DeleteDirAndAllContents(maple_task_dirpath); delete_tmp_dir_err != nil {
		log.Fatalln("Failed to delete maple_task_dirpath and all its contents. Error: ", delete_tmp_dir_err)
	}
}

/*
Creates temporary directory and files for a maple task inside the directory given for this MapleJuiceNode
This opens the output file for the maple task, and returns the file object for it. Caller must close the file descriptor for that file when done.

/task_dirname								(CREATED HERE)

	|- /dataset_dirname  					(CREATED HERE)
		|- dataset files pulled from sdfs 	(NOT CREATED HERE)
	|- maple_task_output_file				(CREATED HERE)
*/
func (this *MapleJuiceNode) createTempDirsAndFilesForMapleTask(taskIndex int, sdfsIntermediateFilenamePrefix string, localWorkerTaskId int) (string, string, *os.File) {
	task_dirpath := filepath.Join(this.nodeTmpDir, fmt.Sprintf(MAPLE_TASK_DIR_NAME_FMT, localWorkerTaskId, taskIndex, sdfsIntermediateFilenamePrefix))
	dataset_dirpath := filepath.Join(task_dirpath, MAPLE_TASK_DATASET_DIR_NAME)
	output_kv_filepath := filepath.Join(task_dirpath, MAPLE_TASK_OUTPUT_FILENAME)

	// create task_dirpath and dataset_dirpath in one call by using MkdirAll()
	if dataset_dir_creation_err := os.MkdirAll(dataset_dirpath, 0744); dataset_dir_creation_err != nil {
		log.Fatalln("Failed to create temporary dataset directory for maple task. Error: ", dataset_dir_creation_err)
	}
	maple_task_output_file, output_file_open_err := os.OpenFile(output_kv_filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0744)
	if output_file_open_err != nil {
		log.Fatalln("Failed to create temporary output file for maple task. Error: ", output_file_open_err)
	}

	return task_dirpath, dataset_dirpath, maple_task_output_file
}

/*
Create a temporary directory for the juice task and a temporary file for the juice task output
What it looks like:

/task_dirpath								(CREATED HERE)

	|- juice_task_output_file				(CREATED HERE)
	|- local sdfs intermediate files for input into juice		(NOT CREATED HERE)

Returns

	task_dirpath (string): path to the temporary directory created for this juice task
	juice_output_file (*os.File): file object for the temporary output file for this juice task
*/
func (this *MapleJuiceNode) createTempDirsAndFilesForJuiceTask(localWorkerId int) (string, *os.File) {
	task_dirpath := filepath.Join(this.nodeTmpDir, fmt.Sprintf(JUICE_TASK_DIR_NAME_FMT, localWorkerId))
	juice_output_filepath := filepath.Join(task_dirpath, JUICE_TASK_OUTPUT_FILENAME)

	if task_dir_creation_err := os.MkdirAll(task_dirpath, 0744); task_dir_creation_err != nil {
		log.Fatalln("Failed to create temporary task directory for juice task. Error: ", task_dir_creation_err)
	}

	juice_output_file, output_file_open_err := os.OpenFile(juice_output_filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0744)
	if output_file_open_err != nil {
		log.Fatalln("Failed to create temporary output file for juice task. Error: ", output_file_open_err)
	}

	return task_dirpath, juice_output_file
}

/*
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

Parameters:

	maple_exe (string): filepath of the maple exe file, exactly as you would type it in the terminal
	args ([]string): additional args to pass to the maple_exe. The arguments should be [num_lines, column_schema, additional_info]
					 at least for now. but that can change if we change the design.
	inputFile (*os.File): file object for the input file that the maple_exe will read from
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

	// wait for program to finish
	if wait_err := cmd.Wait(); wait_err != nil {
		panic(wait_err)
	}

	// write stdout to output file
	// TODO: do i need a file lock since multiple tasks may be writing to the same file?
	// TODO: ^ do we even have multiple goroutines writing to the same file? I don't think so?
	output_writer := bufio.NewWriter(outputFile)
	_, output_write_err := output_writer.Write(stdout_bytes)
	if output_write_err != nil {
		panic(output_write_err)
	}
}
