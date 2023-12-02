package maplejuice

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
*/
type MapleJuiceManager struct {
	id                 NodeID
	sdfsNode           *SDFSNode
	failureJoinService *NodeFailureJoinService
	mjNode             *MapleJuiceNode
}

func NewMapleJuiceManager(id NodeID) *MapleJuiceManager {
	sdfsNode := NewSDFSNode(id)
	failureJoinService := NewNodeFailureJoinService(id)
	mjNode := NewMapleJuiceNode(id)

	return &MapleJuiceManager{
		id:                 id,
		sdfsNode:           sdfsNode,
		failureJoinService: failureJoinService,
		mjNode:             mjNode,
	}
}

func (manager *MapleJuiceManager) Start() {
	// TODO: clear out the maple juice tmp dir contents before creating it (if it exists), and then create it
	manager.failureJoinService.JoinGroup()
	manager.sdfsNode.Start()
	manager.mjNode.Start()
}

func (manager *MapleJuiceManager) HandleNodeFailure(info FailureDetectionInfo) {
	// only handle if we are the leader. cuz otherwise the gossip will eventually send it to the leader
	if nm.sdfsNode.isLeader {
		nm.sdfsNode.leaderService.IndicateNodeFailed(info.FailedNodeId)
	}
	// TODO: add maple juice node worker failure
}

func (manager *MapleJuiceManager) HandleNodeJoin(info NodeJoinInfo) {
	// if a node joined our membership list, i need to reflect that in leaderService.AvailableWorkerNodes
	if nm.sdfsNode.isLeader {
		nm.sdfsNode.leaderService.AddNewActiveNode(info.JoinedNodeId)
	}
}
