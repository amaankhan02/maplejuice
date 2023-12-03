package maplejuice

import (
	"cs425_mp4/internal/config"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
)

type FailureDetectionInfo struct {
	ThisNodeID   NodeID
	FailedNodeId NodeID
}

type NodeJoinInfo struct {
	ThisNodeID   NodeID
	JoinedNodeId NodeID
}

const (
	BUFFER_SIZE = 4096
)

type NodeFailureJoinService struct {
	MembershipList    *MembershipList
	Id                NodeID // Id of this node
	IntroducerId      NodeID
	IsIntroducer      bool // True if this node is the introducer for the group
	Fanout            int  // b
	CurrentGossipMode GossipMode
	ServerListener    net.Listener
	ServerPort        string
	ServerConn        net.PacketConn
	MemListMutexLock  sync.Mutex
	LogFile           *os.File
	IsActive          bool // indicates if the node is currently running
	nodeWaitGroup     sync.WaitGroup
	isTestMode        bool
	msgDropRate       int
	StartTime         int64
	BytesBeingSent    int64 // used to calculate bandwidth
	BytesReceived     int64 // used to calculate bandwidth
	TGossip           int64 // in nanoseconds

	// Add communication medium to the other node types for notifying if we detect a failure
	GossipCallbackHandler INodeManager
}

/*
Constructor for NodeFailureJoinService, initializes with nodeId for this current machine
and creates a socket endpoint for the server and initializes it

If IsIntroducer = true, then IntroducerId is not set to anything
If IsIntroducer = false, then IntroducerId is set to the passed in parameter
*/
func NewNodeFailureJoinService(nodeId NodeID, b int, isIntroducer bool, introducerId NodeID, logFile *os.File, gossipModeValue GossipModeValue,
	isTestMode bool, msgDropRate int, tGossip int64, callbackHandler INodeManager) *NodeFailureJoinService {

	// init the membership list
	thisNode := NodeFailureJoinService{}
	thisNode.Id = nodeId
	thisNode.Fanout = b
	thisNode.IsIntroducer = isIntroducer
	thisNode.LogFile = logFile
	thisNode.CurrentGossipMode = GossipMode{gossipModeValue, 0}
	thisNode.MembershipList = NewMembershipList(nodeId, callbackHandler, isTestMode)
	thisNode.IsActive = false // turns true once JoinGroup() is called
	thisNode.isTestMode = isTestMode
	thisNode.msgDropRate = msgDropRate
	thisNode.BytesBeingSent = 0
	thisNode.BytesReceived = 0
	thisNode.TGossip = tGossip // in nanoseconds
	thisNode.GossipCallbackHandler = callbackHandler

	if !isIntroducer {
		thisNode.IntroducerId = introducerId
		thisNode.MembershipList.AddDefaultEntry(&introducerId)

		// this nodeId has added introducer to its membership list, so its now part of group. Indicate in logs...
		LogMessageln(os.Stdout, "NodeFailureJoinService joining group... Contacted introducer & joined tcp_net")
		LogMessageln(logFile, "NodeFailureJoinService joining group... Contacted introducer & joined tcp_net...")
	} else {
		LogMessageln(os.Stdout, "Introducer has started!")
		LogMessageln(logFile, "Introducer has started!")
	}
	thisNode.initUDPServer()

	return &thisNode
}

//func initializeNewNodeForSDFS(thisNode *NodeFailureJoinService, isLeader bool) *NodeFailureJoinService {
//	// every machine has a FileSystemService and a NewoLDNameNode
//	thisNode.FileSystemService = *NewFileSystemService(thisNode)
//	thisNode.OldNameNode = *NewoLDNameNode(thisNode)
//
//	thisNode.isLeader = isLeader
//
//	return thisNode
//}

func (this *NodeFailureJoinService) JoinGroup() {
	this.StartTime = time.Now().Unix()
	this.IsActive = true

	// spawn gossip failure detection goroutines
	go this.serve()
	go this.heartbeatScheduler()
	go this.periodicChecks()
}

func (this *NodeFailureJoinService) LeaveGroup() {
	this.IsActive = false
	this.nodeWaitGroup.Wait() // wait for server(), heartbeatScheduler(), and periodicChecks() to be done
	programDuration := time.Now().Unix() - this.StartTime

	this.sendLeaveHeartbeats()
	this.shutdownServer()

	LogMessageln(this.LogFile, "Notified others of leaving group. Exiting...")
	LogMessageln(os.Stdout, "Notified others of leaving group. Exiting...")

	if this.isTestMode { // calculate the number of failures and return the failure / duration
		this.logMsgHelper("-------------TEST STATISTICS--------------")

		// false positive rate assumes that there were no real failures during this program's lifetime
		// and therefore all the detected failures were actually false
		falsePositiveRate := float64(this.MembershipList.FalseNodeCount) / float64(programDuration)
		fmtMsg := fmt.Sprintf("%d False Positives in %d seconds\nFalse Positive Rate: %.3f/s\nMessage Drop Rate (percent): %d",
			this.MembershipList.FalseNodeCount, programDuration, falsePositiveRate, this.msgDropRate)
		this.logMsgHelper(fmtMsg)

		bandwidth := float64(this.BytesBeingSent+this.BytesReceived) / float64(programDuration)
		fmtMsg = fmt.Sprintf("Bandwidth: %.4f", bandwidth)
		this.logMsgHelper(fmtMsg)
	}
}

func (this *NodeFailureJoinService) logMsgHelper(msg string) {
	//LogMessageln(os.Stdout, msg)
	LogMessageln(this.LogFile, msg)
}

// initializes the server and boots it up
func (this *NodeFailureJoinService) initUDPServer() {
	conn, err := net.ListenPacket("udp", ":"+this.Id.GossipPort)
	if err != nil {
		log.Fatalln("Error initializing server listener: ", err)
	}

	this.ServerConn = conn
}

func (this *NodeFailureJoinService) periodicNormalModeCheck() {
	currentTime := time.Now().UnixNano()
	for nodeId, memListRow := range this.MembershipList.MemList {
		// evaluate the time of nodeId's last updated time with current time
		if nodeId == this.Id {
			continue // no need to compare with own node id
		} else if memListRow.Status == FAILED || memListRow.Status == LEAVE {
			// DELETE NODE!
			if time.Duration(currentTime-memListRow.LastUpdatedTime) > config.T_CLEANUP {
				err := this.MembershipList.DeleteEntry(&nodeId)
				if err == nil { // successfully deleted
					LogDeletedNodeFromMembershipList(this.LogFile, &nodeId)
					//LogDeletedNodeFromMembershipList(os.Stdout, &nodeId)
				} // if it couldn't delete that's cuz it couldn't find it in the membership list, so we just continue no problem
			}
		} else { // ACTIVE status
			// FAILURE DETECTED!
			if time.Duration(currentTime-memListRow.LastUpdatedTime) > config.T_FAIL_NORMAL {
				LogNodeFail(os.Stdout, &nodeId)
				LogNodeFail(this.LogFile, &nodeId)
				this.MembershipList.UpdateEntry(&nodeId, -1, FAILED, this.LogFile)

				// call the node failure detection handler
				failureInfo := FailureDetectionInfo{ // TODO: figure out later what exact parameters i need to pass
					ThisNodeID:   this.Id,
					FailedNodeId: nodeId,
				}
				this.GossipCallbackHandler.HandleNodeFailure(failureInfo)
			}
		}
	}
}

func (this *NodeFailureJoinService) periodicSuspicionModecheck() {
	currentTime := time.Now().UnixNano()
	for nodeId, memListRow := range this.MembershipList.MemList {
		// evaluate the time of nodeId's last updated time with current time
		if nodeId == this.Id {
			continue // no need to compare with own node id
		} else if memListRow.Status == FAILED || memListRow.Status == LEAVE {
			if time.Duration(currentTime-memListRow.LastUpdatedTime) >= config.T_CLEANUP {
				// DELETE FAILED NODE
				err := this.MembershipList.DeleteEntry(&nodeId)
				if err == nil { // successfully deleted
					LogDeletedNodeFromMembershipList(this.LogFile, &nodeId)
					//LogDeletedNodeFromMembershipList(os.Stdout, &nodeId)
				} // if it couldn't delete that's cuz it couldn't find it in the membership list, so we just continue no problem
			}
		} else if memListRow.Status == SUSPICIOUS {
			if time.Duration(currentTime-memListRow.LastUpdatedTime) >= config.T_FAIL_SUSPICIOUS {
				// MARK AS FAILED
				//LogNodeFail(os.Stdout, &nodeId)
				LogNodeFail(this.LogFile, &nodeId)
				this.MembershipList.UpdateEntry(&nodeId, -1, FAILED, this.LogFile)

				// call the node failure detection handler
				failureInfo := FailureDetectionInfo{ // TODO: figure out later what exact parameters i need to pass
					ThisNodeID:   this.Id,
					FailedNodeId: nodeId,
				}
				this.GossipCallbackHandler.HandleNodeFailure(failureInfo)
			}
		} else { // status = ACTIVE
			if time.Duration(currentTime-memListRow.LastUpdatedTime) >= config.T_SUSPICIOUS {
				// MARK AS SUSPICIOUS
				//LogNodeSuspicious(os.Stdout, &nodeId)
				LogNodeSuspicious(this.LogFile, &nodeId)
				this.MembershipList.UpdateEntry(&nodeId, -1, SUSPICIOUS, this.LogFile)
			}
		}
	}
}

/*
In a loop, it periodically checks its membership list and sees if any node's last updated time
is more than TFAIL time difference from current time. If it is, it changes it to a fail status
and eventually deletes it
*/
func (this *NodeFailureJoinService) periodicChecks() {
	this.nodeWaitGroup.Add(1)
	defer this.nodeWaitGroup.Done()

	for this.IsActive {
		this.MemListMutexLock.Lock()
		if this.CurrentGossipMode.Mode == GOSSIP_NORMAL {
			this.periodicNormalModeCheck()
		} else {
			this.periodicSuspicionModecheck()
		}
		this.MemListMutexLock.Unlock()

		if !this.IsActive { // if not active anymore, then immediately break, don't do time.Sleep()
			break
		}
		time.Sleep(config.T_PERIODIC_CHECK) // wait T_PERIODIC_CHECK time before checking again if any nodes are failed
	}
}

// handles incoming messages to server. UDP Server
func (this *NodeFailureJoinService) serve() {
	this.nodeWaitGroup.Add(1)
	defer this.nodeWaitGroup.Done()

	buffer := make([]byte, BUFFER_SIZE) // TODO: figure out a good value for BUFFER_SIZE

	for this.IsActive {
		n, _, err := this.ServerConn.ReadFrom(buffer) // _ is the addr
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Error reading from UDP: ", err)
			continue
		}

		newGossipMode, recvMembershipList, read_packet_err := ReadGossipUDPPacket(buffer[:n])
		if read_packet_err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}

		this.BytesReceived += int64(n)

		if this.shouldDropMessage() { // drops message based on the msgDropRate
			//LogMessageln(os.Stdout, "Dropped received packet")
			LogMessageln(this.LogFile, "Dropped received packet")
		} else {
			this.TryUpdateGossipMode(newGossipMode)
			go this.handleClientMessage(recvMembershipList)
		}
	}
}

func (this *NodeFailureJoinService) shouldDropMessage() bool {
	randNum := rand.Intn(100)
	return this.isTestMode && randNum < this.msgDropRate
}

/*
Checks if the passed in maplejuice mode is different than the current one. If it is,
it sets it to the new one, and logs message saying that the maplejuice mode has changed. Otherwise,
it doesn't make any change and does not log anything

Even if the new one is not different, but the version number is greater, then it will keep the same
maplejuice mode but will adopt the newer version number

Returns true if the newGossipMode was different from the current and so it changed it successfully.
Otherwise returns false if the newGossipMode was the same as the current.
*/
func (this *NodeFailureJoinService) TryUpdateGossipMode(newGossipMode GossipMode) bool {
	this.MemListMutexLock.Lock()
	defer this.MemListMutexLock.Unlock()

	ret := false
	oldGossipMode := this.CurrentGossipMode

	if newGossipMode.VersionNumber > oldGossipMode.VersionNumber {
		this.CurrentGossipMode = GossipMode{newGossipMode.Mode, newGossipMode.VersionNumber}
		if newGossipMode.Mode != oldGossipMode.Mode { // new one is different than current, then notify
			message := fmt.Sprintf("Gossip Mode changed to %s!", this.CurrentGossipMode.Mode.String())
			//LogMessageln(os.Stdout, message)
			LogMessageln(this.LogFile, message)
			ret = true

			if newGossipMode.Mode == GOSSIP_NORMAL { // prev=Suspicious, new=normal
				// if any nodes in membership list is currently suspicious, re-update them to active
				// since we are disabling suspicion protocol
				this.MembershipList.UpdateSuspicionEntriesToNormal()
			}
		}
	}
	return ret
}

/*
Handles the received client message

TODO: figure out what the actual function parameters for this should be later
*/
func (this *NodeFailureJoinService) handleClientMessage(recvMembList *MembershipList) {
	this.MemListMutexLock.Lock()
	this.MembershipList.Merge(recvMembList, this.LogFile, this.CurrentGossipMode.Mode) // merge this membership list with the received membership list
	LogMembershipList(this.LogFile, this.MembershipList)
	this.MemListMutexLock.Unlock()
}

// loops through, sends heartbeats, and waits T_GOSSIP
// designed to be ran as a goroutine
func (this *NodeFailureJoinService) heartbeatScheduler() {
	this.nodeWaitGroup.Add(1)
	defer this.nodeWaitGroup.Done()

	for this.IsActive {
		this.MemListMutexLock.Lock()
		startTime := time.Now().UnixNano()
		var wg sync.WaitGroup
		this.MembershipList.IncrementHeartbeatCount(&this.Id) // increment hb count of own node
		targets := this.MembershipList.ChooseRandomTargets(this.Fanout, this.Id)

		for _, targetId := range targets {
			wg.Add(1)
			go func(id NodeID) {
				defer wg.Done()
				this.sendHeartbeat(id)
			}(targetId)
		}
		wg.Wait() // wait until all sendHeartbeats() are finished before releasing the lock
		elapsedTime := time.Now().UnixNano() - startTime
		this.MemListMutexLock.Unlock()

		// subtract elapsedTime so that the net result is waiting T_GOSSIP
		waitTime := time.Duration(this.TGossip) - time.Duration(elapsedTime)
		time.Sleep(waitTime)
	}
}

func (this *NodeFailureJoinService) sendLeaveHeartbeats() {
	// update membership list to have this node's status = LEAVE
	// loop through all members in NodeIdsList and send them a heartbeat of the new membership list after

	this.MemListMutexLock.Lock()
	// update membership list to have this node as LEAVE status
	this.MembershipList.UpdateEntry(&this.Id, -1, LEAVE, this.LogFile)
	this.MembershipList.IncrementHeartbeatCount(&this.Id) // increment hb count of own node

	var leaveWg sync.WaitGroup
	targets := this.MembershipList.NodeIdsList // send to all targets
	for _, targetId := range targets {
		leaveWg.Add(1)
		go func(id NodeID) {
			defer leaveWg.Done()
			this.sendHeartbeat(id)
		}(targetId)
	}
	leaveWg.Wait() // wait until all sendHeartbeats() are finished before releasing the lock

	this.MemListMutexLock.Unlock()
}

func (this *NodeFailureJoinService) sendHeartbeat(targetId NodeID) {
	// open connection
	connection, err := net.Dial("udp", targetId.IpAddress+":"+targetId.GossipPort)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error dialing udp connection to %s:%s.\n\tError Message: %v", targetId.IpAddress, targetId.GossipPort, err) // make this log later
		return
	}

	defer func(connection net.Conn) {
		err := connection.Close()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Failed to close connection in sendHeartbeat(), error: ", err)
		}
	}(connection)

	packet, packet_err := CreateGossipUDPPacket(this.CurrentGossipMode, this.MembershipList)
	if packet_err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}

	n, err_write := connection.Write(packet)
	if err_write != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error sending heartbeat over UDP: ", err)
	}
	this.BytesBeingSent += int64(n)

	fmtString := "Sent heartbeat to %s"
	LogMessageln(this.LogFile, fmt.Sprintf(fmtString, targetId.ToStringForGossipLogger()))
}

func (this *NodeFailureJoinService) shutdownServer() {
	err := this.ServerConn.Close()
	if err != nil {
		_, err := fmt.Fprintln(os.Stderr, "Failed to close server connection object")
		if err != nil {
			log.Fatal("Failed to print to stderr - shutdownServer()")
		}
	}
}
