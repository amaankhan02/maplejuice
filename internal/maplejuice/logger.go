package maplejuice

import (
	"cs425_mp4/internal/utils"
	"fmt"
	"log"
	"os"
)

const (
	LOG_OUTPUT_FMT      = "[%s][%s]: %s\n\n"
	JOINED_ACTIVE_MSG   = "JOINED as ACTIVE"
	JOINED_SUS_MSG      = "JOINED as SUSPICIOUS"
	STATUS_CHANGE_FMT   = "Status of NodeFailureJoinService %s changed from %s --> %s"
	FAILED_MSG          = "FAILED"
	SUSPICIOUS_MSG      = "SUSPICIOUS"
	DELETED_MSG         = "DELETED"
	LEFT_MSG            = "NODE LEFT GROUP"
	MEMBERSHIP_LIST_FMT = "[MEMBERSHIP_LIST][%s]:\n%s\n"
	MESSAGE_FMT         = "[%s]: %s\n"
)

func LogNodeFail(stream *os.File, nodeId *NodeID) {
	logHelper(stream, nodeId, FAILED_MSG)
}

func LogNodeJoin(stream *os.File, nodeId *NodeID, status NodeStatus) {
	if status == ACTIVE {
		logHelper(stream, nodeId, JOINED_ACTIVE_MSG)
	} else if status == SUSPICIOUS {
		logHelper(stream, nodeId, JOINED_SUS_MSG)
	}
}

func LogNodeLeft(stream *os.File, nodeId *NodeID) {
	logHelper(stream, nodeId, LEFT_MSG)
}

func LogNodeSuspicious(stream *os.File, nodeId *NodeID) {
	logHelper(stream, nodeId, SUSPICIOUS_MSG)
}

/*
Use this when the status of the node changes.
However, when it changes to FAILED, you should use the LogNodeFail() instead! Don't use this.
This is more so when status changes to ACTIVE or SUSPICIOUS. Code is in place
so that if you pass the new status as FAILED, then it will automatically call LogNodeFail()
*/
func LogNodeStatusChange(stream *os.File, nodeId *NodeID, oldStatus NodeStatus, newStatus NodeStatus) {
	if newStatus == FAILED {
		LogNodeFail(stream, nodeId)
		return
	} else if newStatus == SUSPICIOUS {
		LogNodeSuspicious(stream, nodeId)
		return
	}

	msg := fmt.Sprintf(STATUS_CHANGE_FMT, nodeId.ToStringForGossipLogger(), oldStatus.String(), newStatus.String())
	LogMessageln(stream, msg)
}

func LogMembershipList(stream *os.File, memList *MembershipList) {
	_, err := stream.WriteString(fmt.Sprintf(MEMBERSHIP_LIST_FMT, utils.GetCurrentTime(), memList.String()))
	if err != nil {
		log.Fatal("LogMembershipList(): Failed to WriteString() to the stream")
	}
}

func LogDeletedNodeFromMembershipList(stream *os.File, nodeId *NodeID) {
	logHelper(stream, nodeId, DELETED_MSG)
}

// Logs a message plus the line
func LogMessageln(stream *os.File, message string) {
	_, err := stream.WriteString(fmt.Sprintf(MESSAGE_FMT, utils.GetCurrentTime(), message))
	if err != nil {
		log.Fatal("LogMessageln(): Failed to WriteString() to the stream")
	}
}

// Formats and prints the LOG_OUTPUT_FORMAT variable
func logHelper(stream *os.File, nodeId *NodeID, msgType string) {
	message := fmt.Sprintf(LOG_OUTPUT_FMT, msgType, utils.GetCurrentTime(), nodeId.ToStringForGossipLogger())
	_, errWrite := stream.WriteString(message)
	if errWrite != nil {
		log.Fatal("LogHelper(): Failed to WriteString() to the stream")
	}
}
