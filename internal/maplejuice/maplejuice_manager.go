package maplejuice

import (
	"cs425_mp4/internal/utils"
	"fmt"
	"os"
	"strings"
	"time"
	"strconv"
)

const MAPLE_JUICE_LEADER_DISPATCHER_WAIT_TIME = 500 * time.Millisecond

/*
	INodeManager interface

Defines a generic interface where the HandleFailure() message must
be implemented and HandleNodeJoin()

Current structs that implement INodeManager interface:
  - MJNodeManager
*/
type INodeManager interface {
	HandleNodeFailure(info FailureDetectionInfo)
	HandleNodeJoin(info NodeJoinInfo)
}

/*
	MapleJuiceManager

Manager of various nodes regarding the maple juice program to run.
Implements INodeManager interface

Handles the entire maple juice program. It is the manager of the
SDFSNode, NodeFailureJoinService, and MapleJuiceNode, which are all necessary
for maple juice to run. It also has a parser that parses the command line
input and executes the respective MapleJuiceNode functions.
*/
type MapleJuiceManager struct {
	id                 NodeID
	mapleJuiceNode     *MapleJuiceNode
	sdfsNode           *SDFSNode
	failureJoinService *NodeFailureJoinService
	logFile            *os.File
}

/*
	NewMapleJuiceManager

NOTE: the introducer and leader node are always the same node in this implementation
*/
func NewMapleJuiceManager(
	introducerLeaderVmNum int,
	logFile *os.File,
	sdfsRootDir string,
	mapleJuiceNodeRootDir string,
	gossipFanout int,
	gossipModeValue GossipModeValue,
	tGossip int64,
) *MapleJuiceManager {
	const GOSSIP_IS_TEST_MODE = false // always false for now, set it true later or remove this if we wanna test it out
	const GOSSIP_TEST_MSG_DROP_RATE = 0
	manager := &MapleJuiceManager{}
	localNodeId, introducerLeaderId, isIntroducerLeader := manager.createLocalAndLeaderNodeID(introducerLeaderVmNum)

	sdfsNode := NewSDFSNode(
		*localNodeId,
		*introducerLeaderId,
		isIntroducerLeader,
		logFile,
		sdfsRootDir,
	)
	failureJoinService := NewNodeFailureJoinService(
		*localNodeId,
		gossipFanout,
		isIntroducerLeader,
		*introducerLeaderId,
		logFile,
		gossipModeValue,
		GOSSIP_IS_TEST_MODE,
		GOSSIP_TEST_MSG_DROP_RATE,
		tGossip,
		manager,
	)
	mjNode := NewMapleJuiceNode(
		*localNodeId,
		*introducerLeaderId,
		logFile,
		sdfsNode,
		mapleJuiceNodeRootDir,
		MAPLE_JUICE_LEADER_DISPATCHER_WAIT_TIME,
	)

	manager.id = *localNodeId
	manager.failureJoinService = failureJoinService
	manager.sdfsNode = sdfsNode
	manager.mapleJuiceNode = mjNode

	return manager
}

func (manager *MapleJuiceManager) Start() {
	// remove and clear the directory if it already exists, and then create it
	_ = utils.DeleteDirAndAllContents(manager.mapleJuiceNode.mjRootDir)
	_ = os.Mkdir(manager.mapleJuiceNode.mjRootDir, 0755)
	// create SDFS root directory (delete it first if it already existed)
	_ = os.RemoveAll(manager.sdfsNode.sdfsDir + "/") // remove and clear the directory if it already exists
	_ = os.Mkdir(manager.sdfsNode.sdfsDir, 0755)

	manager.sdfsNode.Start()
	manager.failureJoinService.JoinGroup()
	manager.mapleJuiceNode.Start()

	// LogMembershipList(os.Stdout, manager.failureJoinService.MembershipList)

	manager.startUserInputLoop()
}

func (manager *MapleJuiceManager) createLocalAndLeaderNodeID(introducerLeaderVmNum int) (*NodeID, *NodeID, bool) {
	var introducerLeaderId *NodeID
	var isIntroducerLeader bool
	vmNum, hostname := utils.GetLocalVMInfo()

	if vmNum == introducerLeaderVmNum { // this node is the leader, so don't need to create separate node ID for leader
		isIntroducerLeader = true
		introducerLeaderId = &NodeID{}
	} else { // not the leader, so create the node ID for the leader node
		isIntroducerLeader = false
		introducerHostname := utils.GetHostname(introducerLeaderVmNum)
		introducerLeaderId = NewNodeID(
			utils.GetIP(introducerHostname),
			utils.GetGossipPort(introducerLeaderVmNum),
			utils.GetSDFSPort(introducerLeaderVmNum),
			utils.GetMapleJuicePort(introducerLeaderVmNum),
			true, // we're creating the introducer/leader id so mark it as true
			introducerHostname,
		)
	}
	localNodeId := NewNodeID(
		utils.GetIP(hostname),
		utils.GetGossipPort(vmNum),
		utils.GetSDFSPort(vmNum),
		utils.GetMapleJuicePort(vmNum),
		isIntroducerLeader,
		hostname,
	)
	if isIntroducerLeader {
		introducerLeaderId = localNodeId
	}

	return localNodeId, introducerLeaderId, isIntroducerLeader
}

func (manager *MapleJuiceManager) startUserInputLoop() {
	for {
		fmt.Println()
		input, err := utils.ReadUserInput()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Failed to read user input")
		}

		cmdArgsInput := strings.Fields(input) // split user input up based on spaces
		if shouldExit := manager.executeUserInput(cmdArgsInput); shouldExit {
			break
		}
	}
}

func (manager *MapleJuiceManager) executeUserInput(userInput []string) bool {
	if len(userInput) == 0 {
		return false
	}

	switch strings.ToLower(userInput[0]) {
	case "enable":
		if len(userInput) != 2 {
			fmt.Println("Invalid number of arguments for enable command")
			return false
		}
		if strings.ToLower(userInput[1]) == "suspicion" {
			ok := manager.failureJoinService.TryUpdateGossipMode(GossipMode{
				Mode:          GOSSIP_SUSPICION,
				VersionNumber: manager.failureJoinService.CurrentGossipMode.VersionNumber + 1},
			)
			if !ok {
				fmt.Println("Suspicion mode is already enabled! Nothing to change...")
			}
		} else {
			fmt.Println("Invalid command")
			return false
		}
	case "disable":
		if len(userInput) != 2 {
			fmt.Println("Invalid number of arguments for disable command")
			return false
		}
		if strings.ToLower(userInput[1]) == "suspicion" {
			ok := manager.failureJoinService.TryUpdateGossipMode(GossipMode{
				Mode:          GOSSIP_NORMAL,
				VersionNumber: manager.failureJoinService.CurrentGossipMode.VersionNumber + 1},
			)
			if !ok {
				fmt.Println("Suspicion mode is already disabled! Nothing to change...")
			}
		} else {
			fmt.Println("Invalid command")
			return false
		}
	case "mode":
		fmt.Printf("Current maplejuice mode: %s\n\n", manager.failureJoinService.CurrentGossipMode.Mode.String())
	case "list_mem":
		LogMembershipList(os.Stdout, manager.failureJoinService.MembershipList)
	case "list_self":
		fmt.Println(manager.failureJoinService.Id.ToStringForGossipLogger())
	case "leave":
		manager.failureJoinService.LeaveGroup()
		// NOTE: call respective leave functions for sdfs and maple juice - future improvement
		return true
	case "get":
		if len(userInput) != 3 {
			fmt.Println("Invalid usage. Expected usage: get [sdfs_filename] [local_filename]")
			return false
		}
		sdfsFilename := userInput[1]
		localFilename := userInput[2]
		manager.sdfsNode.PerformGet(sdfsFilename, localFilename)
	case "put":
		if len(userInput) != 3 {
			fmt.Println("Invalid usage. Expected usage: put [local_filename] [sdfs_filename]")
			return false
		}
		localFilename := userInput[1]
		sdfsFilename := userInput[2]
		manager.sdfsNode.PerformPut(localFilename, sdfsFilename)
	case "ls":
		if len(userInput) != 2 {
			fmt.Println("Invalid usage. Expected usage: ls [sdfs_filename]")
			return false
		}
		manager.sdfsNode.PerformLs(userInput[1])
	case "store":
		if len(userInput) != 1 {
			fmt.Println("Invalid usage. Expected usage: store")
			return false
		}
		manager.sdfsNode.PerformStore()
	case "multiread":
		if len(userInput) < 4 {
			fmt.Println("Invalid usage. Expected usage: multiread [sdfs_filename] [local_filename] [VMi], ..., [VMj]")
			return false
		}
		manager.sdfsNode.PerformMultiRead(userInput[1], userInput[2], userInput[3:])
	case "acknowledgement":
		if len(userInput) > 1 {
			fmt.Println("Invalid usage. Expected usage: acknowledgement")
			return false
		}
		manager.sdfsNode.PerformAcknowledgementsPrint()
	case "delete":
		if len(userInput) != 2 {
			fmt.Println("Invalid usage. Expected usage: delete [sdfs_filename]")
			return false
		}
		manager.sdfsNode.PerformDelete(userInput[1])
	case "maple":
		manager.parseAndExecuteMapleInput(userInput)
	}
	return false
}

func (manager *MapleJuiceManager) parseAndExecuteMapleInput(userInput []string) {
	mapleExe := MapleJuiceExeFile{
		ExeFilePath: userInput[1],
	}
	numMaples, parse_err := strconv.Atoi(userInput[2])
	if parse_err != nil {
		fmt.Println("Number of Maples parameter is invalid number!")
		return
	}
	sdfsIntermediateFilenamePrefix := userInput[3]
	sdfsSrcDirectory := userInput[4]

	manager.mapleJuiceNode.SubmitMapleJob(mapleExe, numMaples, sdfsIntermediateFilenamePrefix, sdfsSrcDirectory)
}

func (manager *MapleJuiceManager) HandleNodeFailure(info FailureDetectionInfo) {
	// only handle if we are the leader. cuz otherwise the gossip will eventually send it to the leader
	if manager.sdfsNode.isLeader {
		manager.sdfsNode.leaderService.IndicateNodeFailed(info.FailedNodeId)
	}
}

func (manager *MapleJuiceManager) HandleNodeJoin(info NodeJoinInfo) {
	// if a node joined our membership list, i need to reflect that in leaderService.AvailableWorkerNodes

	if manager.sdfsNode.isLeader {
		manager.sdfsNode.leaderService.AddNewActiveNode(info.JoinedNodeId)
	}
	if manager.mapleJuiceNode.isLeader {
		manager.mapleJuiceNode.leaderService.AddNewAvailableWorkerNode(info.JoinedNodeId)
	}
}
