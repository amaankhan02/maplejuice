package maplejuice

type FailureDetectionInfo struct {
	ThisGossipNodeId NodeID
	FailedNodeId     NodeID
	// TODO: maybe add a OtherData interface{} field
}

type NodeJoinInfo struct {
	ThisGossipNodeId NodeID
	JoinedNodeId     NodeID
}

/*
Defines a generic interface where the HandleFailure() message must
be implemented and HandleNodeJoin()

Current structs that implement NodeGossipHandler interface:
  - SDFSNode
*/
type NodeGossipHandler interface {
	HandleNodeFailure(info FailureDetectionInfo)
	HandleNodeJoin(info NodeJoinInfo)
}
