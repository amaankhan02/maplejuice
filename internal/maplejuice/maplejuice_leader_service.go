package maplejuice

import (
	"bufio"
	"cs425_mp4/internal/datastructures"
	"cs425_mp4/internal/tcp_net"
	"cs425_mp4/internal/utils"
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type JobTaskStatus string
type MapleJuiceJobType int

// output file format for the singular maple task output (the file they send over containing the k,v pairs)
const MAPLE_TASK_OUTPUT_FILENAME_FMT = "male_task_output_%d.csv"           // format: task index

const JUICE_JOB_TEMP_DIR_FMT = "juice_job_%d" 			// format: job id
const JUICE_WORKER_OUTPUT_FILENAME_FMT = "juice_worker_output_%d.csv"      // format: worker node id (like the VM number or IP)
const LOCAL_JUICE_JOB_OUTPUT_FIENAME_FMT = "local_juice_job_output.csv" // format: job id -- this the file that we will send to sdfs as the destination file
// ^ that file is stored inside the JUICE_JOB_TEMP_DIR_FMT directory


const SDFS_INTERMEDIATE_FILE_EXTENSION = ".csv"

const (
	NOT_STARTED JobTaskStatus = "NOT STARTED"
	RUNNING     JobTaskStatus = "RUNNING"
	FINISHED    JobTaskStatus = "FINISHED"

	MAPLE_JOB MapleJuiceJobType = 0
	JUICE_JOB MapleJuiceJobType = 1
)

//type MapleTask struct {
//	//status          JobTaskStatus
//	workerId            NodeID // node id of the worker machine
//	taskIndex           int    // 0-indexed - represents which worker this is so we can calculate which portion of the data it is assigned to
//	totalNumTasksForJob int    // represents the total number of workers for this job
//}

// each worker node id has a list of integers representing the task indices they are assigned.
// the task index is a way for the worker to know which part of the input they have to deal with to be able to split

/*
We assume that there is only 1 job running at a time, therefore we don't need to keep track of the job id.
Future improvement can be to allow multiple jobs running at same time based on some policy.

Whenever we recieve a task response from a worker, we assume its for the current job.

This struct is handled on the leader side. The client cannot decide, for instance, what the job id is since
the job id should be unique to the system, not the client. The client has its own ClientMapleJuiceJob struct
*/
type LeaderMapleJuiceJob struct {
	leaderJobId int
	jobType     MapleJuiceJobType

	// id of the job as seen by the client. This is used when sending a response back to
	// the client to tell it which job is finished (cuz client may send multiple jobs)
	clientJobId int
	clientId    NodeID // id of the client that requested this job

	// for each nodeID, it holds the task indices that it is responsible for. Once the task is completed, it will be removed from this list
	workerToTaskIndices            map[NodeID][]int    // only for maple job
	workerToKeys                   map[NodeID][]string // only for juice job
	numTasks                       int
	exeFile                        MapleJuiceExeFile
	sdfsIntermediateFilenamePrefix string
	numTasksCompleted              int
	numJuiceWorkerNodesCompleted   int                // used ony by juice job (since all juice tasks in a worker are put together in one request)
	sdfsSrcDirectory               string             // only for maple job
	sdfsDestFilename               string             // only for juice job
	delete_input                   bool               // only for juice job
	juicePartitionScheme           JuicePartitionType // only for juice job
	juiceJobOutputFilepath		   string			 // only for juice job
	juiceJobTmpDirPath 			   string			 // only for juice job

	// TODO: implement on map side to update this set of keys
	keys datastructures.HashSet[string] // map job updates this, juice job will look at this later to know what keys are there
	// ! is it fine that I'm storing all the keys in memory? maybe we can store it in a file instead? (future improvement)

	sdfsIntermediateFileMutex sync.Mutex
	sdfsIntermediateFilenames datastructures.HashSet[string] // contains the path to the sdfsIntermediateFiles stored locally
}

/*
Maple Juice Leader Service

Initialized only at the leader node. Handles scheduling of various
MapleJuice jobs

We assume that there is only ever 1 job currently being executed. The others will be queued in FIFO manner
and only executed once the current job finishes.
*/
type MapleJuiceLeaderService struct {
	leaderNodeId		 NodeID
	DispatcherWaitTime   time.Duration
	AvailableWorkerNodes []NodeID // leader cannot be a worker node
	IsRunning            bool
	logFile              *os.File
	waitQueue            []*LeaderMapleJuiceJob
	currentJob           *LeaderMapleJuiceJob
	leaderTempDir        string

	// store the maple jobs that finished, but the juice job has not started/finished yet OR the corresponding
	// juice job didn't tell it to delete the intermediate files yet
	finishedMapleJobs map[string]*LeaderMapleJuiceJob // key=sdfs_intermediate_filename_prefix, value=job
	// TODO: update on map side for when the map job finishes, it should move the job to this map

	jobsSubmitted int // used as the ID for a job. incremented...
	// TODO: ^ increment that in the right places...
}

func NewMapleJuiceLeaderService() *MapleJuiceLeaderService {
	panic("implement me")
}

func (leader *MapleJuiceLeaderService) Start() {
	// todo implement
	go leader.dispatcher()
	panic("implement me")
}

// Submit a maple job to the wait queue. Dispatcher thread will execute it when its ready
func (leader *MapleJuiceLeaderService) SubmitMapleJob(maple_exe MapleJuiceExeFile, num_maples int,
	sdfs_intermediate_filename_prefix string, sdfs_src_dir string, clientJobId int) {
	// TODO: add mutex lock

	job := LeaderMapleJuiceJob{
		leaderJobId:                    leader.jobsSubmitted,
		jobType:                        MAPLE_JOB,
		exeFile:                        maple_exe,
		numTasks:                       num_maples,
		workerToTaskIndices:            make(map[NodeID][]int),
		sdfsIntermediateFilenamePrefix: sdfs_intermediate_filename_prefix,
		sdfsSrcDirectory:               sdfs_src_dir,
		numTasksCompleted:              0,
		sdfsIntermediateFilenames:      make(datastructures.HashSet[string]),
		clientJobId:                    clientJobId,
	}

	leader.waitQueue = append(leader.waitQueue, &job)
	leader.jobsSubmitted++
}

func (leader *MapleJuiceLeaderService) SubmitJuiceJob(juice_exe MapleJuiceExeFile, num_juices int,
	sdfs_intermediate_filename_prefix string, sdfs_dest_filename string, delete_input bool,
	juicePartitionScheme JuicePartitionType, clientJobId int) {

	// TODO: add mutex lock
	job := LeaderMapleJuiceJob{
		leaderJobId:                    leader.jobsSubmitted,
		jobType:                        JUICE_JOB,
		workerToTaskIndices:            make(map[NodeID][]int),
		numTasks:                       num_juices,
		exeFile:                        juice_exe,
		sdfsIntermediateFilenamePrefix: sdfs_intermediate_filename_prefix,
		sdfsDestFilename:               sdfs_dest_filename,
		delete_input:                   delete_input,
		numTasksCompleted:              0,
		juicePartitionScheme:           juicePartitionScheme,
		sdfsIntermediateFilenames:      make(datastructures.HashSet[string]),
		clientJobId:                    clientJobId,
	}
	leader.waitQueue = append(leader.waitQueue, &job)
	leader.jobsSubmitted++
}

/*
Reads the file containing the key value pairs of the task's output from the tcp connection socket 'conn'
Then of the currently running job, it marks the task with the matching taskIndex as finished.
If all tasks have finished, it then proceeds to finish the job by processing the recorded
task output files and then creating a new set of files to be saved in the SDFS file system as

* NOTE: this function reads the file from the connection object, and then closes the connection
*/
func (leader *MapleJuiceLeaderService) ReceiveMapleTaskOutput(workerConn net.Conn, taskIndex int, filesize int64,
	sdfsService *SDFSNode) {

	// todo: ADD MUTEX LOCKS FOR CURRENT_JOB
	// read the task output file from the network
	// TODO: delete these maple task output files after we are done with the job (after we send to sdfs)
	// but for testing purposes, don't delete it
	save_filepath := filepath.Join(leader.leaderTempDir, fmt.Sprintf(MAPLE_TASK_OUTPUT_FILENAME_FMT, taskIndex))
	err := tcp_net.ReadFile(save_filepath, workerConn, filesize)
	if err != nil {
		os.Exit(1) // TODO: for now exit, figure out the best course of action later
	}
	_ = workerConn.Close() // close the connection with this worker since we got all the data we needed from it

	// mark the task as completed now that we got the file
	leader.markTaskAsCompleted(taskIndex)
	leader.currentJob.numTasksCompleted += 1
	// TODO: do we wanna run this on a separate go routine instead?
	if leader.currentJob.jobType == MAPLE_JOB {
		leader.processMapleTaskOutputFile(save_filepath)
	}

	// if all tasks finished, process the output files and save into SDFS
	if leader.currentJob.numTasksCompleted == leader.currentJob.numTasks {
		leader.finishCurrentMapleJob(sdfsService)
	}
}

func (leader *MapleJuiceLeaderService) ReceiveJuiceTaskOutput(workerConn net.Conn, taskAssignedKeys []string, filesize int64, sdfsService *SDFSNode) {
	// TODO: add mutex locks - for current job and other things maybe
	workerVMNumber, _ := utils.GetVMNumber(workerConn.RemoteAddr().String())

	save_filepath := filepath.Join(leader.currentJob.juiceJobTmpDirPath, fmt.Sprintf(JUICE_WORKER_OUTPUT_FILENAME_FMT, workerVMNumber))
	err := tcp_net.ReadFile(save_filepath, workerConn, filesize)
	if err != nil {
		log.Fatalln("Failed to read juice task output file from worker node. Error: ", err)
	}

	// open the save file path to copy the contents over to the JUICE_JOB_OUTPUT_FILEPATH file in append mode
	juiceJobOutputFile, file_err := os.OpenFile(leader.currentJob.juiceJobOutputFilepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if file_err != nil {
		log.Fatalln("Failed to open juice job output file. Error: ", file_err)
	}
	defer juiceJobOutputFile.Close()

	// open the file we just saved from the worker node
	juiceWorkerOutputFile, file2_err := os.OpenFile(save_filepath, os.O_RDONLY, 0444)
	if file2_err != nil {
		log.Fatalln("Failed to open juice worker output file. Error: ", file2_err)
	}
	defer juiceWorkerOutputFile.Close()

	// copy the contents over from the worker output file to the juice job output file in append mode (so we don't overwrite existing data)
	_, copy_err := io.Copy(juiceJobOutputFile, juiceWorkerOutputFile)
	if copy_err != nil {
		log.Fatalln("Failed to copy juice worker output file to juice job output file. Error: ", copy_err)
	}
	
	// mark the task as completed now that we got the file
	leader.currentJob.numJuiceWorkerNodesCompleted += 1
	if leader.currentJob.numJuiceWorkerNodesCompleted == len(leader.currentJob.workerToKeys) {
		// all juice tasks have finished! we can now merge the files and send it to sdfs
		leader.finishCurrentJuiceJob(sdfsService)
	}
}

func (leader *MapleJuiceLeaderService) finishCurrentJuiceJob(sdfsService *SDFSNode) {
	// TODO: do i even need to do a "blocked" put tho? I don't need to wait on the file technically so i could just put it
	sdfsService.PerformPut(leader.currentJob.juiceJobOutputFilepath, leader.currentJob.sdfsDestFilename)

	// delete the intermediate files if the user specified to do so
	if leader.currentJob.delete_input {
		for sdfsIntermediateFilename := range leader.currentJob.sdfsIntermediateFilenames {
			sdfsService.PerformDelete(sdfsIntermediateFilename)
		}
	}

	// establish connection with the client and send a response indicating the job is finished
	clientConn, conn_err := net.Dial("tcp", leader.currentJob.clientId.IpAddress+":"+leader.currentJob.clientId.MapleJuiceServerPort)
	if conn_err != nil {
		fmt.Println("Inside finishCurrentJuiceJob(). ")
		fmt.Println("Failed to connect to client node! Unable to notify client that job is finished. Error: ", conn_err)
	} else {
		SendJuiceJobResponse(clientConn, leader.currentJob.clientJobId) // notify client that job is finished by sending a JOP RESPONSE
	}
	clientConn.Close()

	leader.currentJob = nil
}

func (leader *MapleJuiceLeaderService) finishCurrentMapleJob(sdfsService *SDFSNode) {
	// write intermediate files to SDFS
	for sdfsIntermFilepath := range leader.currentJob.sdfsIntermediateFilenames {
		// TODO: can use the PerformBlockedPuts() function instead so just create a list of the files and then call that function
		sdfsService.PerformBlockedPut(filepath.Join(leader.leaderTempDir, sdfsIntermFilepath), sdfsIntermFilepath)	// TODO: might need to swap the order of the parameters
	}

	// establish connection with the client and send a response indicating the job is finished
	clientConn, conn_err := net.Dial("tcp", leader.currentJob.clientId.IpAddress+":"+leader.currentJob.clientId.MapleJuiceServerPort)
	if conn_err != nil {
		fmt.Println("Inside finishCurrentMapleJob(). ")
		fmt.Println("Failed to connect to client node! Unable to notify client that job is finished. Error: ", conn_err)
	} else {
		SendMapleJobResponse(clientConn, leader.currentJob.clientJobId) // notify client that job is finished by sending a JOP RESPONSE
	}
	clientConn.Close()

	// add the job to the finished maple jobs map so that juice jobs can access it later
	leader.finishedMapleJobs[leader.currentJob.sdfsIntermediateFilenamePrefix] = leader.currentJob
	leader.currentJob = nil
}

/*
Opens up task output file, reads line by line. For each key value pair, it writes to the
correct sdfsIntermediateFile (stored locally for now)
*/
func (this *MapleJuiceLeaderService) processMapleTaskOutputFile(task_output_file string) {
	csvFile, file_err := os.OpenFile(task_output_file, os.O_RDONLY, 0444)
	if file_err != nil {
		return
	}
	csvReader := csv.NewReader(csvFile)

	for {
		record, csv_err := csvReader.Read()
		if csv_err == io.EOF {
			break
		} else if csv_err != nil {
			fmt.Println("Failed to parse CSV file - failed to read line! Exiting...")
			log.Fatalln(csv_err)
		}
		key := record[0]
		value := strings.TrimSuffix(record[1], "\n")

		// open the soon-to-be sdfs_intermediate file and append to it
		_sdfsIntermediateFileName := getSdfsIntermediateFilename(this.currentJob.sdfsIntermediateFilenamePrefix, key)
		fullSdfsIntermediateFilePath := filepath.Join(this.leaderTempDir, _sdfsIntermediateFileName)
		this.currentJob.sdfsIntermediateFilenames.Add(_sdfsIntermediateFileName)

		// TODO: instead of writing to the file every iteration, you can make this a buffered write to make it faster! future improvement!
		// ^ you can first append it to a map, and once the map reaches a certain size you can then write the map to its respective files
		this.currentJob.sdfsIntermediateFileMutex.Lock()
		intermediateFile, file2_err := os.OpenFile(fullSdfsIntermediateFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if file2_err != nil {
			log.Fatalln("Failed to open sdfsIntermediateFileName. Error: ", file2_err)
		}
		writer := bufio.NewWriter(intermediateFile)
		_, interm_write_err := writer.WriteString(key + "," + value + "\n")
		if interm_write_err != nil {
			log.Fatalln("Failed to write key value pair to intermediate file. Error: ", interm_write_err)
		}
		_ = intermediateFile.Close()
		this.currentJob.sdfsIntermediateFileMutex.Unlock()
	}
	_ = csvFile.Close()

	// TODO: delete the task output file since we no longer need it... but for testing purposes don't do it yet... add this as a functionality later
}

// Given the sdfs_intermediate_filename_prefix and the key it will put the 2 together
func getSdfsIntermediateFilename(prefix string, key string) string {
	// TODO: implement this to remove unnallowed characters! - do this later!
	return prefix + "_" + key + SDFS_INTERMEDIATE_FILE_EXTENSION
}

/*
Finds the task with index 'taskIndex' and marks it as completed.

Currently marks it as completed by just removing the taskIndex from the
map of worker nodes to task indices

* this assumes that we have only 1 job running at a time... but we should just simply use a job id instead (future improvement)
*/
func (this *MapleJuiceLeaderService) markTaskAsCompleted(taskIndex int) {
	for workerNodeId, taskIndicesList := range this.currentJob.workerToTaskIndices {
		for i, currTaskIdx := range taskIndicesList {
			if currTaskIdx == taskIndex { // found! -- remove this from the list
				this.currentJob.workerToTaskIndices[workerNodeId] =
					utils.RemoveIthElementFromSlice(this.currentJob.workerToTaskIndices[workerNodeId], i)
				return
			}
		}
	}
}

// TODO: future improvement: allow multiple jobs of the same type to run at the same time (see if any sources of conflicts can happen tho)
func (leader *MapleJuiceLeaderService) dispatcher() {
	for leader.IsRunning {
		if leader.currentJob == nil && len(leader.waitQueue) > 0 {
			// schedule a new one
			leader.currentJob = leader.waitQueue[0]
			leader.waitQueue = leader.waitQueue[1:]

			leader.startJob(leader.currentJob)
		}
		time.Sleep(leader.DispatcherWaitTime)
	}
}

/*
Starts the execution of the job passed in by distributing the tasks among the worker nodes
and sending them task requests to have them begin their work
*/
func (this *MapleJuiceLeaderService) startJob(newJob *LeaderMapleJuiceJob) {
	// TODO: add mutex locks
	this.shuffleAvailableWorkerNodes() // randomize selection of worker nodes each time
	if newJob.jobType == MAPLE_JOB {
		this.mapleAssignTaskIndicesToWorkerNodes(newJob)
		this.sendMapleTasksToWorkerNodes(newJob)
	} else { // JUICE_JOB
		this.getAllKeysForJuiceJob(newJob)
		this.partitionKeysToWorkerNodes(newJob)
		this.sendJuiceTasksToWorkerNodes(newJob)
		this.juiceJobCreateTempDirsAndFiles(newJob)
	}

}

func (this *MapleJuiceLeaderService) juiceJobCreateTempDirsAndFiles(job *LeaderMapleJuiceJob) {
	// create a directory for this job specifically based on its job id
	
	jobTempDir := filepath.Join(this.leaderTempDir, fmt.Sprintf(JUICE_JOB_TEMP_DIR_FMT, job.leaderJobId))
	if jobDirErr := os.Mkdir(jobTempDir, 0755); jobDirErr != nil {
		log.Fatalln("Failed to create directory for juice job. Error: ", jobDirErr)
	}

	// create a file for the final output of the juice job that we store locally before sending to sdfs
	finalJuiceJobOutputFilepath := filepath.Join(jobTempDir, fmt.Sprintf(LOCAL_JUICE_JOB_OUTPUT_FIENAME_FMT))

	job.juiceJobOutputFilepath = finalJuiceJobOutputFilepath
	job.juiceJobTmpDirPath = jobTempDir
}

func (this *MapleJuiceLeaderService) sendJuiceTasksToWorkerNodes(job *LeaderMapleJuiceJob) {
	if job.jobType != JUICE_JOB {
		log.Fatalln("sendJuiceTasksToWorkerNodes() called on a non-juice job!")
	}

	for workerNodeId, assignedKeys := range job.workerToKeys {
		workerConn, conn_err := net.Dial("tcp", workerNodeId.IpAddress+":"+workerNodeId.MapleJuiceServerPort)
		if conn_err != nil {
			fmt.Println("*****Failed to connect to worker node!*****")
			fmt.Println(conn_err)
		} else {
			SendJuiceTaskRequest(workerConn, job.exeFile, job.sdfsIntermediateFilenamePrefix, assignedKeys)
		}
		_ = workerConn.Close()
	}
}

func (this *MapleJuiceLeaderService) getAllKeysForJuiceJob(job *LeaderMapleJuiceJob) {
	// get the corresponding maple job
	mapleJob, ok := this.finishedMapleJobs[job.sdfsIntermediateFilenamePrefix]
	if !ok {
		log.Fatalln("Juice job unable to find a corresponding maple job to get its keys... Exiting...")
	}
	job.keys = mapleJob.keys // copy it over...
}

func (this *MapleJuiceLeaderService) partitionKeysToWorkerNodes(job *LeaderMapleJuiceJob) {
	if job.juicePartitionScheme == HASH_PARTITIONING {
		// get a list of keys assigned to each task. the index of the list represents the task index
		keysForTasks := this.hashPartitionKeysToJuiceTasks(job)

		// now assign the keys to the worker nodes
		for i, keys := range keysForTasks {
			workerNodeId := this.AvailableWorkerNodes[i%len(this.AvailableWorkerNodes)]
			job.workerToKeys[workerNodeId] = keys
		}

	} else if job.juicePartitionScheme == RANGE_PARTITIONING {
		panic("range based partitioning not implemented!") // TODO: add this panic on the client side so that I don't crash the leader.
	} else {
		log.Fatalln("Invalid juice partition scheme! Exiting...")
	}
}

func (this *MapleJuiceLeaderService) hashPartitionKeysToJuiceTasks(job *LeaderMapleJuiceJob) [][]string {
	hasher := fnv.New32()
	tasks := make([][]string, job.numTasks) // create a list of tasks where index = taskIndex and value = list of keys assigned to this task
	for key := range job.keys {
		hasher.Write([]byte(key)) // write the key to the hash

		// compute the hash and assign it to a task number
		taskIndex := int(hasher.Sum32()) % job.numTasks
		if tasks[taskIndex] == nil {
			tasks[taskIndex] = make([]string, 0)
		}
		tasks[taskIndex] = append(tasks[taskIndex], key) // add key to this juice task
	}

	return tasks
}

func (this *MapleJuiceLeaderService) shuffleAvailableWorkerNodes() {
	// Fisher-Yates shuffle (https://yourbasic.org/golang/shuffle-slice-array/)
	for i := len(this.AvailableWorkerNodes) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		this.AvailableWorkerNodes[i], this.AvailableWorkerNodes[j] = this.AvailableWorkerNodes[j], this.AvailableWorkerNodes[i]
	}
}

/*
Helper function used for maple.

Given the number of maple tasks, it assigns each task to a worker node for those nodes to
be contacted later
*/
func (this *MapleJuiceLeaderService) mapleAssignTaskIndicesToWorkerNodes(job *LeaderMapleJuiceJob) {
	for i := 0; i < job.numTasks; i++ { // i = task index
		currWorkerIdx := i % len(this.AvailableWorkerNodes)
		currWorkerNodeId := this.AvailableWorkerNodes[currWorkerIdx]

		taskIndicesList, ok := job.workerToTaskIndices[currWorkerNodeId]
		if !ok { // create the list of tasks if this worker has not been assigned a task yet
			job.workerToTaskIndices[currWorkerNodeId] = make([]int, 0)
			taskIndicesList, _ = job.workerToTaskIndices[currWorkerNodeId]
		}
		taskIndicesList = append(taskIndicesList, i)
	}
}

func (this *MapleJuiceLeaderService) sendMapleTasksToWorkerNodes(job *LeaderMapleJuiceJob) {
	if job.jobType != MAPLE_JOB {
		log.Fatalln("sendMapleTasksToWorkerNodes() called on a non-maple job!")
	}

	for workerNodeId, taskIndices := range job.workerToTaskIndices {
		workerConn, conn_err := net.Dial("tcp", workerNodeId.IpAddress+":"+workerNodeId.MapleJuiceServerPort)
		if conn_err != nil {
			fmt.Println("*****Failed to connect to worker node!*****")
			fmt.Println(conn_err)
			// TODO: is this the best way to handle? we should probably abort right? or maybe just continue with the other nodes?
			// * we would have to move those task indices to another worker node tho to make sure the job is executed
			continue
		}

		for _, taskIndex := range taskIndices {
			SendMapleTaskRequest(
				workerConn,
				job.numTasks,
				job.exeFile,
				job.sdfsIntermediateFilenamePrefix,
				job.sdfsSrcDirectory,
				taskIndex,
			)
		}

		_ = workerConn.Close()
	}
}
