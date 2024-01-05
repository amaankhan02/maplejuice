package core

import "log"

type NodeStatus int

// NodeStatus values
const (
	ACTIVE     NodeStatus = 0
	FAILED     NodeStatus = 1
	SUSPICIOUS NodeStatus = 2
	LEAVE      NodeStatus = 3
)

func (status NodeStatus) String() string {
	if status == ACTIVE {
		return "ACTIVE"
	} else if status == FAILED {
		return "FAILED"
	} else if status == SUSPICIOUS {
		return "SUSPICIOUS"
	} else if status == LEAVE {
		return "LEAVE"
	} else {
		log.Fatal("Invalid NodeStatus!")
		return ""
	}
}
