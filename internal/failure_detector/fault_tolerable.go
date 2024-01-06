package failure_detector

import "cs425_mp4/internal/core"

type FailureDetectionInfo struct {
	ThisNodeID   core.NodeID
	FailedNodeId core.NodeID
}

type NodeJoinInfo struct {
	ThisNodeID   core.NodeID
	JoinedNodeId core.NodeID
}

/*
	FaultTolerable interface

Defines a generic interface where the HandleFailure() message must
be implemented and HandleNodeJoin()

Current structs that implement FaultTolerable interface:
  - maplejuice.MapleJuiceNodeManager
*/
type FaultTolerable interface {
	HandleNodeFailure(info FailureDetectionInfo)
	HandleNodeJoin(info NodeJoinInfo)
}
