package maplejuice

import (
	"cs425_mp4/internal/utils"
	"os"
)

const GOSSIP_IS_TEST_MODE = false // always false for now, set it true later or remove this if we wanna test it out

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
	sdfsNode           *SDFSNode
	failureJoinService *NodeFailureJoinService
	mjNode             *MapleJuiceNode
	logFile            *os.File
}

func NewMapleJuiceManager(introducerLeaderVmNum int, logFile *os.File, sdfsRootDir string, gossipFanout int,
	gossipModeValue GossipModeValue, tGossip int64) *MapleJuiceManager {
	// NOTE: the introducer and leader node are always the same node in this implementation
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

	manager := &MapleJuiceManager{}

	manager.id = *NewNodeID(
		utils.GetIP(hostname),
		utils.GetGossipPort(vmNum),
		utils.GetSDFSPort(vmNum),
		utils.GetMapleJuicePort(vmNum),
		isIntroducerLeader,
		hostname,
	)
	sdfsNode := NewSDFSNode(
		manager.id,
		*introducerLeaderId,
		isIntroducerLeader,
		logFile,
		sdfsRootDir,
	)
	failureJoinService := NewNodeFailureJoinService(
		manager.id,
		gossipFanout,
		isIntroducerLeader,
		*introducerLeaderId,
		logFile,
		gossipModeValue,
		GOSSIP_IS_TEST_MODE,
		0,
		tGossip,
		manager,
	)
	manager.sdfsNode = sdfsNode
	manager.failureJoinService = failureJoinService

	return manager
}

func (manager *MapleJuiceManager) Start() {
	// TODO: clear out the maple juice tmp dir contents before creating it (if it exists), and then create it
	manager.failureJoinService.JoinGroup()
	manager.sdfsNode.Start()
	manager.mjNode.Start()

	manager.startUserInputLoop()
}

func (manager *MapleJuiceManager) startUserInputLoop() {
	panic("implement me")
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
}
