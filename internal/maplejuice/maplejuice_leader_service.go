package maplejuice

import (
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
	status   JobTaskStatus
	workerId NodeID // id of the worker machine

	// todo: add more information on the portion of the data that is assigned to this task
}

type MapleJob struct {
	tasks                          []MapleTask
	exeFile                        string
	sdfsIntermediateFilenamePrefix string
	sdfsSrcDirectory               string
	status                         JobTaskStatus
}

/*
Maple Juice Leader Service

Initialized only at the leader node. Handles scheduling of various
MapleJuice jobs
*/
type MapleJuiceLeaderService struct {
	DispatcherWaitTime time.Duration
	ActiveNodes        []NodeID
	IsRunning          bool
	logFile            *os.File
	waitQueue          []*MapleJob
	currentMapleJob    *MapleJob
}

func (this *MapleJuiceLeaderService) Start() {
	// todo implement
	panic("implement me")
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

}

// Submit a maple job to the wait queue. Dispatcher thread will execute it when its ready
func (this *MapleJuiceLeaderService) SubmitMapleJob(maple_exe string, sdfs_intermediate_filename_prefix string,
	sdfs_src_dir string) {
	job := MapleJob{
		exeFile:                        maple_exe,
		sdfsIntermediateFilenamePrefix: sdfs_intermediate_filename_prefix,
		sdfsSrcDirectory:               sdfs_src_dir,
	}

	// workers will be chosen when the job is ready to be executed
	this.waitQueue = append(this.waitQueue, &job)
}
