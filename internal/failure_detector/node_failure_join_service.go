package failure_detector

import (
	"cs425_mp4/internal/config"
	"cs425_mp4/internal/core"
	"encoding/csv"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

const (
	BUFFER_SIZE = 4096
	//DATA_ANALYSIS_FILENAME = "gossip_data_analytics.csv"
)

var DATA_ANALYSIS_CSV_HEADER = []string{"test_type", "gossip_mode", "fanout", "t_fail", "t_gossip", "num_nodes", "msg_drop_rate", "bandwidth", "false_positive_rate", "program_duration"}

// NodeFailureJoinServiceTestingParams Used for testing (gather data for analysis)
type NodeFailureJoinServiceTestingParams struct {
	IsTestMode         bool
	TestType           string
	MsgDropRate        int
	AppendToDataFile   bool
	DataOutputFileName string
}

type NodeFailureJoinService struct {
	MembershipList    *MembershipList
	Id                core.NodeID // Id of this node
	IntroducerId      core.NodeID
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

	testModeParams NodeFailureJoinServiceTestingParams
	StartTime      int64
	BytesBeingSent int64 // used to calculate bandwidth
	BytesReceived  int64 // used to calculate bandwidth
	TGossip        int64 // in nanoseconds

	// Add communication medium to the other node types for notifying if we detect a failure
	GossipCallbackHandler FaultTolerable
}

/*
Constructor for NodeFailureJoinService, initializes with nodeId for this current machine
and creates a socket endpoint for the server and initializes it

If IsIntroducer = true, then IntroducerId is not set to anything
If IsIntroducer = false, then IntroducerId is set to the passed in parameter
*/
func NewNodeFailureJoinService(
	nodeId core.NodeID,
	isIntroducer bool,
	introducerId core.NodeID,
	logFile *os.File,
	gossipModeValue GossipModeValue,
	tGossip int64,
	testingParams NodeFailureJoinServiceTestingParams,
	callbackHandler FaultTolerable,
) *NodeFailureJoinService {

	// init the membership list
	thisNode := NodeFailureJoinService{}
	thisNode.Id = nodeId
	thisNode.Fanout = config.FANOUT
	thisNode.IsIntroducer = isIntroducer
	thisNode.LogFile = logFile
	thisNode.CurrentGossipMode = GossipMode{gossipModeValue, 0}
	thisNode.MembershipList = NewMembershipList(nodeId, callbackHandler, testingParams.IsTestMode)
	thisNode.IsActive = false // turns true once JoinGroup() is called
	thisNode.BytesBeingSent = 0
	thisNode.BytesReceived = 0
	thisNode.TGossip = tGossip // in nanoseconds
	thisNode.GossipCallbackHandler = callbackHandler
	thisNode.testModeParams = testingParams

	if !isIntroducer {
		thisNode.IntroducerId = introducerId
		thisNode.MembershipList.AddDefaultEntry(&introducerId)

		// this nodeId has added introducer to its membership list, so its now part of group. Indicate in logs...
		core.LogMessageln(os.Stdout, "NodeFailureJoinService joining group... Contacted introducer & joined tcp_net")
		core.LogMessageln(logFile, "NodeFailureJoinService joining group... Contacted introducer & joined tcp_net...")
	} else {
		core.LogMessageln(os.Stdout, "Introducer has started!")
		core.LogMessageln(logFile, "Introducer has started!")
	}
	thisNode.initUDPServer()

	return &thisNode
}

func (nfjs *NodeFailureJoinService) JoinGroup() {
	nfjs.StartTime = time.Now().Unix()
	nfjs.IsActive = true

	// spawn gossip failure detection goroutines
	go nfjs.serve()
	go nfjs.heartbeatScheduler()
	go nfjs.periodicChecks()
}

func (nfjs *NodeFailureJoinService) LeaveGroup() {
	nfjs.IsActive = false
	nfjs.nodeWaitGroup.Wait() // wait for server(), heartbeatScheduler(), and periodicChecks() to be done
	programDuration := time.Now().Unix() - nfjs.StartTime

	nfjs.sendLeaveHeartbeats()
	nfjs.shutdownServer()

	core.LogMessageln(nfjs.LogFile, "Notified others of leaving group. Exiting...")
	core.LogMessageln(os.Stdout, "Notified others of leaving group. Exiting...")

	if nfjs.testModeParams.IsTestMode {
		nfjs.saveDataAnalysisInfo(programDuration)
	}
}

func (nfjs *NodeFailureJoinService) saveDataAnalysisInfo(programDuration int64) {
	dataOutputFilePath, _ := filepath.Abs(filepath.Join(config.APP_ROOT_DIR, nfjs.testModeParams.DataOutputFileName))

	// Open/Create the data output file
	var outputCsvFile *os.File
	var fileOpenErr error
	var createdNewFile = false
	if _, err := os.Stat(dataOutputFilePath); os.IsNotExist(err) {
		createdNewFile = true
	}
	outputCsvFile, fileOpenErr = os.OpenFile(dataOutputFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if fileOpenErr != nil {
		log.Fatal("Failed to create/open data output file: ", fileOpenErr)
	}
	defer func(outputCsvFile *os.File) {
		_ = outputCsvFile.Close()
	}(outputCsvFile)

	// Prepare to write to the file
	writer := csv.NewWriter(outputCsvFile)
	defer writer.Flush()

	if createdNewFile { // write the header
		if err := writer.Write(DATA_ANALYSIS_CSV_HEADER); err != nil {
			log.Fatal("Failed to write header to csv data output file: ", err)
		}
	}

	// compute and write the data record
	// {"test_type", "gossip_mode", "fanout", "t_fail",
	// "t_gossip", "num_nodes", "msg_drop_rate", "bandwidth", "false_positive_rate", "program_duration"}

	// compute values and write
	var gossip_mode string
	var t_fail string
	if nfjs.CurrentGossipMode.Mode == GOSSIP_NORMAL {
		gossip_mode = "normal"
		t_fail = config.T_FAIL_NORMAL.String()
	} else {
		gossip_mode = "suspicious"
		t_fail = (config.T_SUSPICIOUS + config.T_FAIL_SUSPICIOUS).String()
	}
	numNodes := strconv.Itoa(nfjs.MembershipList.GetNumNodes())
	msgDropRate := strconv.Itoa(nfjs.testModeParams.MsgDropRate)
	bandwidth := strconv.FormatFloat(float64(nfjs.BytesBeingSent+nfjs.BytesReceived)/float64(programDuration), 'f', 4, 64)

	// false positive rate assumes that there were no real failures during this program's lifetime
	// and therefore all the detected failures were actually false
	falsePositiveRate := strconv.FormatFloat(float64(nfjs.MembershipList.FalseNodeCount)/float64(programDuration), 'f', 4, 64)
	writeErr := writer.Write([]string{
		nfjs.testModeParams.TestType,
		gossip_mode,
		strconv.Itoa(nfjs.Fanout),
		t_fail,
		strconv.FormatInt(nfjs.TGossip, 10),
		numNodes,
		msgDropRate,
		bandwidth,
		falsePositiveRate,
		strconv.FormatInt(programDuration, 10),
	})
	if writeErr != nil {
		log.Fatalf("Failed to write data record to csv data output file: %s", writeErr)
	}
}

func (nfjs *NodeFailureJoinService) logMsgHelper(msg string) {
	//LogMessageln(os.Stdout, msg)
	core.LogMessageln(nfjs.LogFile, msg)
}

// initializes the server and boots it up
func (nfjs *NodeFailureJoinService) initUDPServer() {
	conn, err := net.ListenPacket("udp", ":"+nfjs.Id.GossipPort)
	if err != nil {
		log.Fatalln("Error initializing server listener: ", err)
	}

	nfjs.ServerConn = conn
}

func (nfjs *NodeFailureJoinService) periodicNormalModeCheck() {
	currentTime := time.Now().UnixNano()
	for nodeId, memListRow := range nfjs.MembershipList.MemList {
		// evaluate the time of nodeId's last updated time with current time
		if nodeId == nfjs.Id {
			continue // no need to compare with own node id
		} else if memListRow.Status == core.FAILED || memListRow.Status == core.LEAVE {
			// DELETE NODE!
			if time.Duration(currentTime-memListRow.LastUpdatedTime) > config.T_CLEANUP {
				err := nfjs.MembershipList.DeleteEntry(&nodeId)
				if err == nil { // successfully deleted
					core.LogDeletedNodeFromMembershipList(nfjs.LogFile, &nodeId)
					//LogDeletedNodeFromMembershipList(os.Stdout, &nodeId)
				} // if it couldn't delete that's cuz it couldn't find it in the membership list, so we just continue no problem
			}
		} else { // ACTIVE status
			// FAILURE DETECTED!
			if time.Duration(currentTime-memListRow.LastUpdatedTime) > config.T_FAIL_NORMAL {
				core.LogNodeFail(os.Stdout, &nodeId)
				core.LogNodeFail(nfjs.LogFile, &nodeId)
				nfjs.MembershipList.UpdateEntry(&nodeId, -1, core.FAILED, nfjs.LogFile)

				// call the node failure detection handler
				failureInfo := FailureDetectionInfo{ // TODO: figure out later what exact parameters i need to pass
					ThisNodeID:   nfjs.Id,
					FailedNodeId: nodeId,
				}
				nfjs.GossipCallbackHandler.HandleNodeFailure(failureInfo)
			}
		}
	}
}

func (nfjs *NodeFailureJoinService) periodicSuspicionModecheck() {
	currentTime := time.Now().UnixNano()
	for nodeId, memListRow := range nfjs.MembershipList.MemList {
		// evaluate the time of nodeId's last updated time with current time
		if nodeId == nfjs.Id {
			continue // no need to compare with own node id
		} else if memListRow.Status == core.FAILED || memListRow.Status == core.LEAVE {
			if time.Duration(currentTime-memListRow.LastUpdatedTime) >= config.T_CLEANUP {
				// DELETE FAILED NODE
				err := nfjs.MembershipList.DeleteEntry(&nodeId)
				if err == nil { // successfully deleted
					core.LogDeletedNodeFromMembershipList(nfjs.LogFile, &nodeId)
					//LogDeletedNodeFromMembershipList(os.Stdout, &nodeId)
				} // if it couldn't delete that's cuz it couldn't find it in the membership list, so we just continue no problem
			}
		} else if memListRow.Status == core.SUSPICIOUS {
			if time.Duration(currentTime-memListRow.LastUpdatedTime) >= config.T_FAIL_SUSPICIOUS {
				// MARK AS FAILED
				//LogNodeFail(os.Stdout, &nodeId)
				core.LogNodeFail(nfjs.LogFile, &nodeId)
				nfjs.MembershipList.UpdateEntry(&nodeId, -1, core.FAILED, nfjs.LogFile)

				// call the node failure detection handler
				failureInfo := FailureDetectionInfo{ // TODO: figure out later what exact parameters i need to pass
					ThisNodeID:   nfjs.Id,
					FailedNodeId: nodeId,
				}
				nfjs.GossipCallbackHandler.HandleNodeFailure(failureInfo)
			}
		} else { // status = ACTIVE
			if time.Duration(currentTime-memListRow.LastUpdatedTime) >= config.T_SUSPICIOUS {
				// MARK AS SUSPICIOUS
				//LogNodeSuspicious(os.Stdout, &nodeId)
				core.LogNodeSuspicious(nfjs.LogFile, &nodeId)
				nfjs.MembershipList.UpdateEntry(&nodeId, -1, core.SUSPICIOUS, nfjs.LogFile)
			}
		}
	}
}

/*
In a loop, it periodically checks its membership list and sees if any node's last updated time
is more than TFAIL time difference from current time. If it is, it changes it to a fail status
and eventually deletes it
*/
func (nfjs *NodeFailureJoinService) periodicChecks() {
	nfjs.nodeWaitGroup.Add(1)
	defer nfjs.nodeWaitGroup.Done()

	for nfjs.IsActive {
		nfjs.MemListMutexLock.Lock()
		if nfjs.CurrentGossipMode.Mode == GOSSIP_NORMAL {
			nfjs.periodicNormalModeCheck()
		} else {
			nfjs.periodicSuspicionModecheck()
		}
		nfjs.MemListMutexLock.Unlock()

		if !nfjs.IsActive { // if not active anymore, then immediately break, don't do time.Sleep()
			break
		}
		time.Sleep(config.T_PERIODIC_CHECK) // wait T_PERIODIC_CHECK time before checking again if any nodes are failed
	}
}

// handles incoming messages to server. UDP Server
func (nfjs *NodeFailureJoinService) serve() {
	nfjs.nodeWaitGroup.Add(1)
	defer nfjs.nodeWaitGroup.Done()

	buffer := make([]byte, BUFFER_SIZE) // TODO: figure out a good value for BUFFER_SIZE

	for nfjs.IsActive {
		n, _, err := nfjs.ServerConn.ReadFrom(buffer) // _ is the addr
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Error reading from UDP: ", err)
			continue
		}

		newGossipMode, recvMembershipList, read_packet_err := ReadGossipUDPPacket(buffer[:n])
		if read_packet_err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			continue
		}

		nfjs.BytesReceived += int64(n)

		if nfjs.shouldDropMessage() { // drops message based on the msgDropRate
			//LogMessageln(os.Stdout, "Dropped received packet")
			core.LogMessageln(nfjs.LogFile, "Dropped received packet")
		} else {
			nfjs.TryUpdateGossipMode(newGossipMode)
			go nfjs.handleClientMessage(recvMembershipList)
		}
	}
}

func (nfjs *NodeFailureJoinService) shouldDropMessage() bool {
	randNum := rand.Intn(100)
	return nfjs.testModeParams.IsTestMode && randNum < nfjs.testModeParams.MsgDropRate
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
func (nfjs *NodeFailureJoinService) TryUpdateGossipMode(newGossipMode GossipMode) bool {
	nfjs.MemListMutexLock.Lock()
	defer nfjs.MemListMutexLock.Unlock()

	ret := false
	oldGossipMode := nfjs.CurrentGossipMode

	if newGossipMode.VersionNumber > oldGossipMode.VersionNumber {
		nfjs.CurrentGossipMode = GossipMode{newGossipMode.Mode, newGossipMode.VersionNumber}
		if newGossipMode.Mode != oldGossipMode.Mode { // new one is different than current, then notify
			message := fmt.Sprintf("Gossip Mode changed to %s!", nfjs.CurrentGossipMode.Mode.String())
			//LogMessageln(os.Stdout, message)
			core.LogMessageln(nfjs.LogFile, message)
			ret = true

			if newGossipMode.Mode == GOSSIP_NORMAL { // prev=Suspicious, new=normal
				// if any nodes in membership list is currently suspicious, re-update them to active
				// since we are disabling suspicion protocol
				nfjs.MembershipList.UpdateSuspicionEntriesToNormal()
			}
		}
	}
	return ret
}

/*
Handles the received client message
*/
func (nfjs *NodeFailureJoinService) handleClientMessage(recvMembList *MembershipList) {
	nfjs.MemListMutexLock.Lock()
	nfjs.MembershipList.Merge(recvMembList, nfjs.LogFile, nfjs.CurrentGossipMode.Mode) // merge this membership list with the received membership list
	LogMembershipList(nfjs.LogFile, nfjs.MembershipList)
	nfjs.MemListMutexLock.Unlock()
}

// loops through, sends heartbeats, and waits T_GOSSIP
// designed to be ran as a goroutine
func (nfjs *NodeFailureJoinService) heartbeatScheduler() {
	nfjs.nodeWaitGroup.Add(1)
	defer nfjs.nodeWaitGroup.Done()

	for nfjs.IsActive {
		nfjs.MemListMutexLock.Lock()
		startTime := time.Now().UnixNano()
		var wg sync.WaitGroup
		nfjs.MembershipList.IncrementHeartbeatCount(&nfjs.Id) // increment hb count of own node
		targets := nfjs.MembershipList.ChooseRandomTargets(nfjs.Fanout, nfjs.Id)

		for _, targetId := range targets {
			wg.Add(1)
			go func(id core.NodeID) {
				defer wg.Done()
				nfjs.sendHeartbeat(id)
			}(targetId)
		}
		wg.Wait() // wait until all sendHeartbeats() are finished before releasing the lock
		elapsedTime := time.Now().UnixNano() - startTime
		nfjs.MemListMutexLock.Unlock()

		// subtract elapsedTime so that the net result is waiting T_GOSSIP
		waitTime := time.Duration(nfjs.TGossip) - time.Duration(elapsedTime)
		time.Sleep(waitTime)
	}
}

func (nfjs *NodeFailureJoinService) sendLeaveHeartbeats() {
	// update membership list to have this node's status = LEAVE
	// loop through all members in NodeIdsList and send them a heartbeat of the new membership list after

	nfjs.MemListMutexLock.Lock()
	// update membership list to have this node as LEAVE status
	nfjs.MembershipList.UpdateEntry(&nfjs.Id, -1, core.LEAVE, nfjs.LogFile)
	nfjs.MembershipList.IncrementHeartbeatCount(&nfjs.Id) // increment hb count of own node

	var leaveWg sync.WaitGroup
	targets := nfjs.MembershipList.NodeIdsList // send to all targets
	for _, targetId := range targets {
		leaveWg.Add(1)
		go func(id core.NodeID) {
			defer leaveWg.Done()
			nfjs.sendHeartbeat(id)
		}(targetId)
	}
	leaveWg.Wait() // wait until all sendHeartbeats() are finished before releasing the lock

	nfjs.MemListMutexLock.Unlock()
}

func (nfjs *NodeFailureJoinService) sendHeartbeat(targetId core.NodeID) {
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

	packet, packet_err := CreateGossipUDPPacket(nfjs.CurrentGossipMode, nfjs.MembershipList)
	if packet_err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}

	n, err_write := connection.Write(packet)
	if err_write != nil {
		_, _ = fmt.Fprintln(os.Stderr, "Error sending heartbeat over UDP: ", err)
	}
	nfjs.BytesBeingSent += int64(n)

	fmtString := "Sent heartbeat to %s"
	core.LogMessageln(nfjs.LogFile, fmt.Sprintf(fmtString, targetId.ToStringForGossipLogger()))
}

func (nfjs *NodeFailureJoinService) shutdownServer() {
	err := nfjs.ServerConn.Close()
	if err != nil {
		_, err := fmt.Fprintln(os.Stderr, "Failed to close server connection object")
		if err != nil {
			log.Fatal("Failed to print to stderr - shutdownServer()")
		}
	}
}
