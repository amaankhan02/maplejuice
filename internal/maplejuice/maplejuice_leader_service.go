package maplejuice

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
)

type JobTaskStatus string
type MapleJuiceJobType int

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
	workerToTaskIndices            map[NodeID][]int
	numTasks                       int
	exeFile                        string
	sdfsIntermediateFilenamePrefix string
	numTasksCompleted              int
	sdfsSrcDirectory               string             // only for maple job
	sdfsDestFilename               string             // only for juice job
	delete_input                   bool               // only for juice job
	juicePartitionScheme           JuicePartitionType // only for juice job
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
	}

	// workers will be chosen when the job is ready to be executed
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
	}
	this.waitQueue = append(this.waitQueue, &job)
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
