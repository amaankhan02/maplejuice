package maplejuice

/*
Defines a generic interface where the HandleFailure() message must
be implemented and HandleNodeJoin()

Current structs that implement INodeManager interface:
  - SDFSNode
*/
type INodeManager interface {
	HandleNodeFailure(info FailureDetectionInfo)
	HandleNodeJoin(info NodeJoinInfo)
}

/*
Manager of various nodes regarding the maple juice program to run.

Implements INodeManager interface
*/
type MJNodeManager struct {
	id                 NodeID
	sdfsNode           *SDFSNode
	failureJoinService *NodeFailureJoinService
	mjNode             *MapleJuiceNode
}

func (nm *MJNodeManager) NewMJNodeManager(isTestMode bool, msgDropRate int, tGossip int64) {
	//nm.failureJoinService = NewFailureJoinService(nm.id,
	//	config.FANOUT,
	//	isIntroducerLeader,
	//	introducerLeaderId,
	//	logFile,
	//	GOSSIP_NORMAL,
	//	isTestMode,
	//	msgDropRate,
	//	tGossip,
	//	sdfsNode,
	//)
}

func (nm *MJNodeManager) Start() {
	nm.failureJoinService.JoinGroup()
	nm.sdfsNode.Start()
	//nm.mjNode.Start()
}

func (nm *MJNodeManager) HandleNodeFailure(info FailureDetectionInfo) {
	// only handle if we are the leader. cuz otherwise the gossip will eventually send it to the leader
	if nm.sdfsNode.isLeader {
		nm.sdfsNode.leaderService.IndicateNodeFailed(info.FailedNodeId)
	}
	// TODO: add maple juice node worker failure
}

func (nm *MJNodeManager) HandleNodeJoin(info NodeJoinInfo) {
	// if a node joined our membership list, i need to reflect that in leaderService.ActiveNodes
	if nm.sdfsNode.isLeader {
		nm.sdfsNode.leaderService.AddNewActiveNode(info.JoinedNodeId)
	}
}
