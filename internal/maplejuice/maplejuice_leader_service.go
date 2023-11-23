package maplejuice

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
)

type JobTaskStatus string

const (
	NOT_STARTED JobTaskStatus = "NOT STARTED"
	RUNNING     JobTaskStatus = "RUNNING"
	FINISHED    JobTaskStatus = "FINISHED"
)

type MapleTask struct {
	//status          JobTaskStatus
	workerId            NodeID // node id of the worker machine
	taskIndex           int    // 0-indexed - represents which worker this is so we can calculate which portion of the data it is assigned to
	totalNumTasksForJob int    // represents the total number of workers for this job
}

type MapleJob struct {
	//tasks                          []MapleTask
	workerToTasks                  map[NodeID][]MapleTask // records the maple tasks, in a map w/ key = node id that is responsible for that maple task
	numTasks                       int
	exeFile                        string
	sdfsIntermediateFilenamePrefix string
	sdfsSrcDirectory               string
	//status                         JobTaskStatus
	numTasksCompleted int
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
	waitQueue            []*MapleJob
	currentMapleJob      *MapleJob
}

func (this *MapleJuiceLeaderService) Start() {
	// todo implement
	panic("implement me")
}

// Submit a maple job to the wait queue. Dispatcher thread will execute it when its ready
func (this *MapleJuiceLeaderService) SubmitMapleJob(maple_exe string, num_maples int, sdfs_intermediate_filename_prefix string,
	sdfs_src_dir string) {
	job := MapleJob{
		exeFile:  maple_exe,
		numTasks: num_maples,
		//tasks:                          make([]MapleTask, num_maples),
		workerToTasks:                  make(map[NodeID][]MapleTask),
		sdfsIntermediateFilenamePrefix: sdfs_intermediate_filename_prefix,
		sdfsSrcDirectory:               sdfs_src_dir,
		numTasksCompleted:              0,
	}

	// workers will be chosen when the job is ready to be executed
	this.waitQueue = append(this.waitQueue, &job)
}

func (this *MapleJuiceLeaderService) dispatcher() {
	for this.IsRunning {
		if this.currentMapleJob == nil && len(this.waitQueue) > 0 {
			// schedule a new one
			this.currentMapleJob = this.waitQueue[0]
			this.waitQueue = this.waitQueue[1:]

			this.startMapleJob(this.currentMapleJob)
		}
		time.Sleep(this.DispatcherWaitTime)
	}
}

/*
Starts the execution of the current job
*/
func (this *MapleJuiceLeaderService) startMapleJob(newJob *MapleJob) {
	// TODO: we dont even need a MapleTask struct... all we're doing is sending the job info plus the task number...
	// assign tasks to the worker nodes
	this.shuffleAvailableWorkerNodes() // shuffle to randomize the selection
	for i := 0; i < newJob.numTasks; i++ {
		currWorkerIdx := i % len(this.AvailableWorkerNodes)
		currWorkerNodeId := this.AvailableWorkerNodes[currWorkerIdx]

		tasksList, ok := newJob.workerToTasks[currWorkerNodeId]
		if !ok { // create the list of tasks if this worker has not been assigned a task yet
			newJob.workerToTasks[currWorkerNodeId] = make([]MapleTask, 0)
			tasksList, _ = newJob.workerToTasks[currWorkerNodeId]
		}
		tasksList = append(tasksList,
			MapleTask{
				workerId:            currWorkerNodeId,
				taskIndex:           i,
				totalNumTasksForJob: newJob.numTasks,
			})
	}

	// contact worker nodes and send them a task request
	for workerNodeId, tasksList := range newJob.workerToTasks {
		workerConn, conn_err := net.Dial("tcp", workerNodeId.IpAddress+":"+workerNodeId.MapleJuiceServerPort)
		if conn_err != nil {
			fmt.Println("*****Failed to connect to worker node!*****")
			fmt.Println(conn_err)
			// TODO: is this the best way to handle? we should probably abort right? or maybe just continue with the other nodes?
			continue
		}

		for _, currTask := range tasksList {
			msg := MapleJuiceNetworkMessage{
				MsgType:                        MAPLE_TASK_REQUEST,
				NumTasks:                       newJob.numTasks,
				ExeFile:                        newJob.exeFile,
				SdfsIntermediateFilenamePrefix: newJob.sdfsIntermediateFilenamePrefix,
				SdfsSrcDirectory:               newJob.sdfsSrcDirectory,
				CurrTaskIdx:                    currTask.taskIndex,
			}
			SendMJNetworkMessage(workerConn, &msg)
		}

		workerConn.Close()
	}
}

func (this *MapleJuiceLeaderService) shuffleAvailableWorkerNodes() {
	// Fisher-Yates shuffle (https://yourbasic.org/golang/shuffle-slice-array/)
	for i := len(this.AvailableWorkerNodes) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		this.AvailableWorkerNodes[i], this.AvailableWorkerNodes[j] = this.AvailableWorkerNodes[j], this.AvailableWorkerNodes[i]
	}
}
