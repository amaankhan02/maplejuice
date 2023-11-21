package sdfs

import (
	"cs425_mp3/internal/utils"
	"fmt"
	"strconv"
	"time"
)

type NodeID struct {
	IpAddress      string
	GossipPort     string // UDP port for the gossip failure detection communication
	SDFSServerPort string // TCP port for the SDFS protocol communication
	Timestamp      int64
	Hostname       string
}

const DELIMINATOR = ";"

func NewNodeID(ip string, gossipPort string, sdfsPort string, isIntroducerLeader bool, hostname string) *NodeID {
	var timestamp int64
	if isIntroducerLeader {
		timestamp = 0
	} else {
		timestamp = time.Now().UnixNano()
	}

	return &NodeID{
		IpAddress:      ip,
		GossipPort:     gossipPort,
		SDFSServerPort: sdfsPort,
		Timestamp:      timestamp,
		Hostname:       hostname,
	}
}

// Converts NodeID object to a formatted string
func (nodeid *NodeID) ToStringMP2() string {
	return nodeid.IpAddress + DELIMINATOR + nodeid.GossipPort + DELIMINATOR + strconv.FormatInt(nodeid.Timestamp, 10)
}

// Returns NodeID in string format for MP2 specification
func (nodeid *NodeID) ToStringForGossipLogger() string {
	fmtString := "VM%02d (%s:%s:%019d)"
	vm_num, _ := utils.GetVMNumber(nodeid.Hostname)
	return fmt.Sprintf(fmtString, vm_num, nodeid.IpAddress, nodeid.GossipPort, nodeid.Timestamp)
}

func (nodeid *NodeID) ToStringMP3() string {
	fmtString := "VM%02d - %s - %s"
	vm_num, _ := utils.GetVMNumber(nodeid.Hostname)
	return fmt.Sprintf(fmtString, vm_num, nodeid.Hostname, nodeid.IpAddress)
}

//func NodeIdFromString(strNodeId string) *NodeID {
//	nodeIdParams := strings.Split(strNodeId, DELIMINATOR)
//
//	Timestamp, err := strconv.ParseInt(nodeIdParams[2], 10, 64)
//	if err != nil {
//		fmt.Println("Error in getting the Timestamp when converting to GossipNode Id from string")
//	}
//
//	if len(nodeIdParams) > 3 {
//		fmt.Println("Error in converting to GossipNode Id from String")
//	}
//
//	nodeId := NodeID{
//		nodeIdParams[0],
//		nodeIdParams[1],
//		Timestamp,
//		nodeIdParams[3],
//	}
//
//	return &nodeId
//}

func NodesAreEqual(nodeId1 *NodeID, nodeId2 *NodeID) bool {
	if nodeId1.IpAddress == nodeId2.IpAddress &&
		nodeId1.GossipPort == nodeId2.GossipPort &&
		nodeId1.Timestamp == nodeId2.Timestamp {
		return true
	}

	return false
}
