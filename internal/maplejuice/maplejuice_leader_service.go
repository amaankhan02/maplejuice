package maplejuice

import (
	"bufio"
	"cs425_mp4/internal/datastructures"
	"cs425_mp4/internal/tcp_net"
	"cs425_mp4/internal/utils"
	"encoding/csv"
	"fmt"
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
const MAPLE_TASK_OUTPUT_FILENAME_FMT = "task_output_%d.csv"

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

type MapleJuiceJob struct {
	// each worker node id has a list of integers representing the task indices they are assigned.
	// the task index is a way for the worker to know which part of the input they have to deal with to be able to split
	jobType                        MapleJuiceJobType
	clientId                       NodeID // id of the client that requested this job
	workerToTaskIndices            map[NodeID][]int
	numTasks                       int
	exeFile                        string
	sdfsIntermediateFilenamePrefix string
	numTasksCompleted              int
	sdfsSrcDirectory               string             // only for maple job
	sdfsDestFilename               string             // only for juice job
	delete_input                   bool               // only for juice job
	juicePartitionScheme           JuicePartitionType // only for juice job

	sdfsIntermediateFileMutex sync.Mutex
	sdfsIntermediateFilenames datastructures.HashSet[string] // contains the path to the sdfsIntermediateFiles stored locally
}

/*
Maple Juice Leader Service

Initialized only at the leader node. Handles scheduling of various
MapleJuice jobs
*/
type MapleJuiceLeaderService struct {
	DispatcherWaitTime   time.Duration
	AvailableWorkerNodes []NodeID // leader cannot be a worker node
	IsRunning            bool
	logFile              *os.File
	waitQueue            []*MapleJuiceJob
	currentJob           *MapleJuiceJob
	leaderTempDir        string
}

func NewMapleJuiceLeaderService() *MapleJuiceLeaderService {
	panic("implement me")
}

func (this *MapleJuiceLeaderService) Start() {
	// todo implement
	panic("implement me")
}

// Submit a maple job to the wait queue. Dispatcher thread will execute it when its ready
func (this *MapleJuiceLeaderService) SubmitMapleJob(maple_exe string, num_maples int,
	sdfs_intermediate_filename_prefix string, sdfs_src_dir string) {

	job := MapleJuiceJob{
		jobType:                        MAPLE_JOB,
		exeFile:                        maple_exe,
		numTasks:                       num_maples,
		workerToTaskIndices:            make(map[NodeID][]int),
		sdfsIntermediateFilenamePrefix: sdfs_intermediate_filename_prefix,
		sdfsSrcDirectory:               sdfs_src_dir,
		numTasksCompleted:              0,
		sdfsIntermediateFilenames:      make(datastructures.HashSet[string]),
	}

	this.waitQueue = append(this.waitQueue, &job)
}

func (this *MapleJuiceLeaderService) SubmitJuiceJob(juice_exe string, num_juices int,
	sdfs_intermediate_filename_prefix string, sdfs_dest_filename string, delete_input bool,
	juicePartitionScheme JuicePartitionType) {

	// TODO: add mutex lock
	job := MapleJuiceJob{
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
	}
	this.waitQueue = append(this.waitQueue, &job)
}

/*
Reads the file containing the key value pairs of the task's output from the tcp connection socket 'conn'
Then of the currently running job, it marks the task with the matching taskIndex as finished.
If all tasks have finished, it then proceeds to finish the job by processing the recorded
task output files and then creating a new set of files to be saved in the SDFS file system as
*/
func (this *MapleJuiceLeaderService) ReceiveMapleTaskOutput(conn net.Conn, taskIndex int, filesize int64,
	sdfsService *SDFSNode) {
	// read the task output file from the network
	save_filepath := filepath.Join(this.leaderTempDir, fmt.Sprintf(MAPLE_TASK_OUTPUT_FILENAME_FMT, taskIndex))
	err := tcp_net.ReadFile(save_filepath, conn, filesize)
	if err != nil {
		os.Exit(1) // TODO: for now exit, figure out the best course of action later
	}

	// mark the task as completed now that we got the file
	this.markTaskAsCompleted(taskIndex)
	this.currentJob.numTasksCompleted += 1
	if this.currentJob.jobType == MAPLE_JOB {
		this.processMapleTaskOutputFile(save_filepath)
	}

	// if all tasks finished, process the output files and save into SDFS
	if this.currentJob.numTasksCompleted == this.currentJob.numTasks {
		this.finishCurrentJob(sdfsService)
	}
}

func (this *MapleJuiceLeaderService) finishCurrentJob(sdfsService *SDFSNode) {
	// write intermediate files to SDFS
	for sdfsIntermFilepath := range this.currentJob.sdfsIntermediateFilenames {
		sdfsService.PerformPut(filepath.Join(this.leaderTempDir, sdfsIntermFilepath), sdfsIntermFilepath)
		// TODO!: need to add a way for me to get notified that the put operation was completed... the acknowledgement i receive must get a callback? maybe a listener? idrk... must figure out
		// cuz after i know it's done i can delete interm local files...
		// if that's too hard, i can just store these files in a separate folder, unique by the job id like "job 0"
		// and then just have the entire directory deleted after my program is done. cuz space is not really an issue...
		// or pass in a boolean into PerformPut() to have it block or not until it gets the ACK, and in this case we can block until it gets the ACK...
	}
	this.currentJob = nil
	// TODO: send an ACK back to the client acknowledging that its done?

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
	return prefix + "_" + key + ".csv"
}

/*
Finds the task with index 'taskIndex' and marks it as completed.

Currently marks it as completed by just removing the taskIndex from the
map of worker nodes to task indices
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

func (this *MapleJuiceLeaderService) dispatcher() {
	for this.IsRunning {
		if this.currentJob == nil && len(this.waitQueue) > 0 {
			// schedule a new one
			this.currentJob = this.waitQueue[0]
			this.waitQueue = this.waitQueue[1:]

			this.startJob(this.currentJob)
		}
		time.Sleep(this.DispatcherWaitTime)
	}
}

/*
Starts the execution of the current job by distributing the tasks among the worker nodes
and sending them task requests to have them begin their work
*/
func (this *MapleJuiceLeaderService) startJob(newJob *MapleJuiceJob) {
	// TODO: add mutex locks
	this.shuffleAvailableWorkerNodes() // randomize selection of worker nodes each time
	this.assignTaskIndicesToWorkerNodes(newJob)
	this.sendTasksToWorkerNodes(newJob)
}

func (this *MapleJuiceLeaderService) shuffleAvailableWorkerNodes() {
	// Fisher-Yates shuffle (https://yourbasic.org/golang/shuffle-slice-array/)
	for i := len(this.AvailableWorkerNodes) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		this.AvailableWorkerNodes[i], this.AvailableWorkerNodes[j] = this.AvailableWorkerNodes[j], this.AvailableWorkerNodes[i]
	}
}

func (this *MapleJuiceLeaderService) assignTaskIndicesToWorkerNodes(job *MapleJuiceJob) {
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

func (this *MapleJuiceLeaderService) sendTasksToWorkerNodes(job *MapleJuiceJob) {
	for workerNodeId, taskIndices := range job.workerToTaskIndices {
		workerConn, conn_err := net.Dial("tcp", workerNodeId.IpAddress+":"+workerNodeId.MapleJuiceServerPort)
		if conn_err != nil {
			fmt.Println("*****Failed to connect to worker node!*****")
			fmt.Println(conn_err)
			// TODO: is this the best way to handle? we should probably abort right? or maybe just continue with the other nodes?
			continue
		}

		for _, taskIndex := range taskIndices {
			if job.jobType == MAPLE_JOB {
				SendMapleTaskRequest(
					workerConn,
					job.numTasks,
					job.exeFile,
					job.sdfsIntermediateFilenamePrefix,
					job.sdfsSrcDirectory,
					taskIndex,
				)
			} else {
				SendJuiceTaskRequest(
					workerConn,
					job.numTasks,
					job.exeFile,
					job.sdfsIntermediateFilenamePrefix,
					job.sdfsDestFilename,
					job.delete_input,
					job.juicePartitionScheme,
					taskIndex,
				)
			}

		}

		_ = workerConn.Close()
	}
}
