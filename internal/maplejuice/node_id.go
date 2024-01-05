package maplejuice

import (
	"cs425_mp4/internal/utils"
	"fmt"
	"strconv"
	"time"
)

type NodeID struct {
	IpAddress            string
	Hostname             string
	GossipPort           string // UDP port for the gossip failure detection communication
	SDFSServerPort       string // TCP port for the SDFS protocol communication
	MapleJuiceServerPort string // TCP port for the MapleJuice protocol communication
	Timestamp            int64  // Unix Nano timestamp of when this peer/machine was initialized
}

const DELIMINATOR = ";"

func NewNodeID(ip string, gossipPort string, sdfsPort string, mapleJuicePort string, isIntroducerLeader bool, hostname string) *NodeID {
	var timestamp int64
	if isIntroducerLeader {
		timestamp = 0
	} else {
		timestamp = time.Now().UnixNano()
	}

	return &NodeID{
		IpAddress:            ip,
		GossipPort:           gossipPort,
		SDFSServerPort:       sdfsPort,
		MapleJuiceServerPort: mapleJuicePort,
		Timestamp:            timestamp,
		Hostname:             hostname,
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

func NodesAreEqual(nodeId1 *NodeID, nodeId2 *NodeID) bool {
	if nodeId1.IpAddress == nodeId2.IpAddress &&
		nodeId1.GossipPort == nodeId2.GossipPort &&
		nodeId1.Timestamp == nodeId2.Timestamp {
		return true
	}

	return false
}
