package maplejuice

import (
	"bytes"
	"encoding/gob"
	"cs425_mp4/internal/core"
	"cs425_mp4/internal/utils"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"
)

// Private struct for MembershipList values
// Immutable data structure
type MembershipListEntry struct {
	HeartBeatCount  int
	LastUpdatedTime int64 // time in which an entry in this local membershiplist was last updated (based on local clock)
	Status          core.NodeStatus
}

func (m MembershipListEntry) String() string {
	fmtString := "%03d  |  %d  |  %s"
	return fmt.Sprintf(fmtString, m.HeartBeatCount, m.LastUpdatedTime, m.Status.String())
}

type MembershipList struct {
	MemList         map[core.NodeID]MembershipListEntry
	RoundRobinPtr   int      // index
	ThisNodeId      core.NodeID   // for the current machine this nodeId is for
	NodeIdsList     []core.NodeID // list of nodes you can send message to -- used for round-robin messaging
	// gossipMode      GossipMode
	FalseNodeCount  int64 // used for testing - for false positive rate calculation
	CallbackHandler INodeManager
	IsTestMode      bool
}

// --------------------- STATIC FUNCTIONS -----------------------

/*
Constructor for MembershipList.
Initializes MembershipList struct with the following parameters and
returns a pointer to the object

Args:

	ThisNodeId (core.NodeID): core.NodeID of this current node of this machine
*/
func NewMembershipList(thisNodeId core.NodeID, callbackHandler INodeManager, isTestMode bool) *MembershipList {

	// intialize membership list
	memList := &MembershipList{
		MemList:         make(map[core.NodeID]MembershipListEntry),
		RoundRobinPtr:   0,
		ThisNodeId:      thisNodeId,
		NodeIdsList:     []core.NodeID{thisNodeId},
		CallbackHandler: callbackHandler,
		IsTestMode:      isTestMode,
	}

	// Initialize the map (actual membership list)
	memList.MemList[thisNodeId] = MembershipListEntry{
		HeartBeatCount:  0,
		LastUpdatedTime: time.Now().UnixNano(),
		Status:          core.ACTIVE,
	}
	memList.FalseNodeCount = 0
	return memList
}

// Returns the MembershipList in the form of a string
func (memList *MembershipList) String() string {
	var sb strings.Builder
	fmtString := "%s\t|  %s\n"
	for nodeId, membershipListRow := range memList.MemList {
		sb.WriteString(fmt.Sprintf(fmtString, nodeId.ToStringForGossipLogger(), membershipListRow.String()))
	}
	return sb.String()
}

/*
Serializes the MembershipList object, HOWEVER, it only serializes
the MemList map, since sending the round-robin pointer and the
thisNodeID is not necessary. It just takes up unnecessary tcp_net bandwidth

TODO: see if ^^ that is necessary, or if we should just serialize the entire thing
*/
func (memList *MembershipList) SerializeMembershipList() ([]byte, error) {
	binaryBuff := new(bytes.Buffer)
	encoder := gob.NewEncoder(binaryBuff)
	err := encoder.Encode(memList.MemList)

	if err != nil {
		return nil, err
	}

	return binaryBuff.Bytes(), nil
}

/*
Deserializees the passed in byte array representing the membershipList map
and then sets the memList.MemList to that.
*/
func (memList *MembershipList) DeserializeMembershipListMap(membershipListData []byte) error {
	var membershipList map[core.NodeID]MembershipListEntry

	byteBuffer := bytes.NewBuffer(membershipListData)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(&membershipList)
	if err != nil {
		return err
	}

	memList.MemList = membershipList
	return nil
}

// -------------------- INSTANCE FUNCTIONS ------------------------

func (thisList *MembershipList) MergeNormal(otherList *MembershipList, logStream *os.File) {
	for nodeId, incomingMemlistRow := range otherList.MemList {
		if nodeId == thisList.ThisNodeId { // no need to compare its own node's to itself
			continue
		}
		thisMemListRow, exists := thisList.MemList[nodeId] // get corresponding key-value pair in this memlist

		if !exists { // found new Node Joined
			if incomingMemlistRow.Status == core.ACTIVE { // the new node is not failed, so lets add it as a join
				logJoinHelper(logStream, &nodeId, core.ACTIVE)
				thisList.AddEntry(&nodeId, incomingMemlistRow.HeartBeatCount, core.ACTIVE)

				thisList.CallbackHandler.HandleNodeJoin(NodeJoinInfo{
					ThisNodeID:   thisList.ThisNodeId,
					JoinedNodeId: nodeId,
				})

			} else { // the new node is a failed node, and it doesn't exist here, so we just continue
				continue // since this node was never introduced to that node in the membership list, so we don't know abt it
			}
		} else { // node exists in our local membership list
			if incomingMemlistRow.Status == core.LEAVE {
				if thisMemListRow.Status != core.LEAVE && incomingMemlistRow.Status > thisMemListRow.Status {
					core.LogNodeLeft(os.Stdout, &nodeId)
					core.LogNodeLeft(logStream, &nodeId)
					thisList.UpdateEntry(&nodeId, -1, core.LEAVE, logStream) // indicate core.LEAVE to eventually delete after T_CLEANUP
				}
			} else if thisMemListRow.Status == core.FAILED {
				continue // implementing FAIL-STOP Model - so don't do anything if it's already marked failed
			} else { // this entry = core.ACTIVE
				if incomingMemlistRow.Status == core.FAILED { // this=active, incoming=failed
					if incomingMemlistRow.HeartBeatCount > thisMemListRow.HeartBeatCount {
						// listen to the incoming --> make ours failed as well
						logFailHelper(logStream, &nodeId)
						thisList.UpdateEntry(&nodeId, -1, core.FAILED, logStream)

						thisList.CallbackHandler.HandleNodeFailure(FailureDetectionInfo{
							ThisNodeID:   thisList.ThisNodeId,
							FailedNodeId: nodeId,
						})

					} else {
						continue // we have the most updated version - so do nothing
					}
				} else { // incoming = active
					if incomingMemlistRow.HeartBeatCount > thisMemListRow.HeartBeatCount {
						// listen to the incoming --> this & incoming is alive, w/ incoming has higher hb, so update
						thisList.UpdateEntry(&nodeId, incomingMemlistRow.HeartBeatCount, -1, logStream)
					} else {
						continue // both are active, but we have most updated version, so don't update anything
					}
				}
			}
		}
	}
}

func (thisList *MembershipList) MergeSuspicion(otherList *MembershipList, logStream *os.File) {
	for nodeId, incomingMemlistRow := range otherList.MemList {
		if nodeId == thisList.ThisNodeId {
			continue
		}
		thisMemListRow, exists := thisList.MemList[nodeId]

		if !exists { // New Node Joined
			if incomingMemlistRow.Status == core.ACTIVE {
				logJoinHelper(logStream, &nodeId, core.ACTIVE)
				thisList.AddEntry(&nodeId, incomingMemlistRow.HeartBeatCount, core.ACTIVE)

				thisList.CallbackHandler.HandleNodeJoin(NodeJoinInfo{
					ThisNodeID:   thisList.ThisNodeId,
					JoinedNodeId: nodeId,
				})
			} else if incomingMemlistRow.Status == core.SUSPICIOUS {
				logJoinHelper(logStream, &nodeId, core.SUSPICIOUS)
				thisList.AddEntry(&nodeId, incomingMemlistRow.HeartBeatCount, core.SUSPICIOUS)

				thisList.CallbackHandler.HandleNodeJoin(NodeJoinInfo{
					ThisNodeID:   thisList.ThisNodeId,
					JoinedNodeId: nodeId,
				})
			} else { // incoming = core.FAILED
				continue // does not exist locally, but incoming says its failed. so we have nothing to do
			}
		} else if incomingMemlistRow.HeartBeatCount > thisMemListRow.HeartBeatCount { // nodeId exists locally
			// we only wanna update w/ incoming if its HB count is greater. Otherwise we don't change and keep ours
			if incomingMemlistRow.Status == core.LEAVE { // node exists locally and incoming = core.LEAVE
				if thisMemListRow.Status == core.LEAVE { // if its already marked leave here, then don't do anything
					continue
				}
				core.LogNodeLeft(os.Stdout, &nodeId)
				core.LogNodeLeft(logStream, &nodeId)
				thisList.UpdateEntry(&nodeId, -1, core.LEAVE, logStream) // indicate core.LEAVE to eventually delete after T_CLEANUP
			} else if thisMemListRow.Status == incomingMemlistRow.Status {
				thisList.UpdateEntry(&nodeId, incomingMemlistRow.HeartBeatCount, -1, logStream)
			} else if thisMemListRow.Status == core.ACTIVE {
				if incomingMemlistRow.Status == core.SUSPICIOUS {
					core.LogNodeSuspicious(logStream, &nodeId)
					core.LogNodeSuspicious(os.Stdout, &nodeId)
					thisList.UpdateEntry(&nodeId, incomingMemlistRow.HeartBeatCount, core.SUSPICIOUS, logStream)
				} else { // incoming = core.FAILED
					logFailHelper(logStream, &nodeId)
					thisList.UpdateEntry(&nodeId, incomingMemlistRow.HeartBeatCount, core.FAILED, logStream)

					thisList.CallbackHandler.HandleNodeFailure(FailureDetectionInfo{
						ThisNodeID:   thisList.ThisNodeId,
						FailedNodeId: nodeId,
					})
				}
			} else if thisMemListRow.Status == core.SUSPICIOUS {
				if incomingMemlistRow.Status == core.ACTIVE {
					core.LogNodeStatusChange(os.Stdout, &nodeId, core.SUSPICIOUS, core.ACTIVE)
					core.LogNodeStatusChange(logStream, &nodeId, core.SUSPICIOUS, core.ACTIVE)
					thisList.UpdateEntry(&nodeId, incomingMemlistRow.HeartBeatCount, core.ACTIVE, logStream)
				} else { // incoming = core.FAILED
					logFailHelper(logStream, &nodeId)
					thisList.UpdateEntry(&nodeId, incomingMemlistRow.HeartBeatCount, core.FAILED, logStream)

					thisList.CallbackHandler.HandleNodeFailure(FailureDetectionInfo{
						ThisNodeID:   thisList.ThisNodeId,
						FailedNodeId: nodeId,
					})
				}
			} else { // this = core.FAILED
				continue // implementing FAIL-STOP MODEL
			}
		}
	}
}

/*
Merge this list with otherList
if logStream != nil, then it logs messages to logStream file.
*/
func (thisList *MembershipList) Merge(otherList *MembershipList, logStream *os.File, gossipModeVal GossipModeValue) {
	if gossipModeVal == GOSSIP_NORMAL {
		thisList.MergeNormal(otherList, logStream)
	} else if gossipModeVal == GOSSIP_SUSPICION {
		thisList.MergeSuspicion(otherList, logStream)
	} else {
		log.Fatal("Invalid GossipModeValue passed in. Cannot merge")
	}
}

/*
Convert all the current suspicious nodes to core.ACTIVE status. It will also update its last-updated-time as well.
This function is used when we are in suspicious mode and we disable it. So that way any currently suspicious nodes
are just re-updated to normal
*/
func (thisList *MembershipList) UpdateSuspicionEntriesToNormal() {
	for nodeId, memListRow := range thisList.MemList {
		if memListRow.Status == core.SUSPICIOUS {
			thisList.UpdateEntry(&nodeId, -1, core.ACTIVE, nil)
		}
	}
}

/*
Updates the membership list row (entry) for key=nodeId with the new passed in heartbeat and status
It also updates the last updated time to be the new current local time since the entry was updated

If newHeartbeatCount == -1, then it won't update the heartbeat
If newStatus == -1, then it won't update the status
*/
func (memList *MembershipList) UpdateEntry(nodeId *core.NodeID, newHeartbeatCount int, newStatus core.NodeStatus, logStream *os.File) {
	oldRow, exists := memList.MemList[*nodeId]

	if !exists {
		log.Fatal("UpdateEntry(): nodeId does not exist in the map!")
	}

	if newHeartbeatCount == -1 {
		newHeartbeatCount = oldRow.HeartBeatCount
	}
	if newStatus == -1 {
		newStatus = oldRow.Status
	}

	if oldRow.Status != core.FAILED && newStatus == core.FAILED {
		memList.FalseNodeCount++
		// if changed to failed, it will also remove the nodeId from the nodeIdsList
		memList.NodeIdsList = RemoveElementFromSlice(memList.NodeIdsList, *nodeId)

		// log the detection time here
		if logStream != nil {
			detectionTimeMs := float64(time.Now().UnixNano()-oldRow.LastUpdatedTime) / 1e6 // convert to ms
			if memList.IsTestMode {
				core.LogMessageln(logStream, fmt.Sprintf("[DETECTION TIME] %.2f", detectionTimeMs))
				core.LogMessageln(os.Stdout, fmt.Sprintf("[DETECTION TIME] %.2f", detectionTimeMs))
			}

		}
	}

	memList.MemList[*nodeId] = MembershipListEntry{
		newHeartbeatCount,
		time.Now().UnixNano(),
		newStatus,
	}

}

/*
Adds a new key-value pair to the membership list. With the key as the nodeID,
and the value as the membershiplistrow entry. Additionally fills the last updated
time with the current time.

It addtionally adds the new node id to the list of nodeids
*/
func (memList *MembershipList) AddEntry(nodeId *core.NodeID, heartBeatCount int, status core.NodeStatus) {
	// making sure id does not already exist in the map, if it does, return error
	_, exists := memList.MemList[*nodeId]
	if exists {
		//return errors.New("AddEntry(): ID already exists in map")
		log.Fatal("AddEntry(): ID already exists in the map - cannot add new entry!")
	}

	memList.MemList[*nodeId] = MembershipListEntry{
		HeartBeatCount:  heartBeatCount,
		LastUpdatedTime: time.Now().UnixNano(),
		Status:          status,
	}
	memList.NodeIdsList = append(memList.NodeIdsList, *nodeId)
}

/*
Adds an entry to the membershiplist with key=nodeId, and the entry values as default values, which
are heartbeatcount = 0, and status = ALIVE, and the current time right now as the last updated time
*/
func (memList *MembershipList) AddDefaultEntry(nodeId *core.NodeID) {
	memList.AddEntry(nodeId, 0, core.ACTIVE)
}

/*
Deletes an entry from the membership list
*/
func (memList *MembershipList) DeleteEntry(nodeId *core.NodeID) error {
	entry, exists := memList.MemList[*nodeId]
	if !exists {
		err := errors.New("DeleteEntry(): ID does not exist in map")
		return err
	}
	if entry.Status == core.FAILED {
		delete(memList.MemList, *nodeId) // if its failed, then it was already removed from the nodeIdsList
	} else {
		delete(memList.MemList, *nodeId)
		memList.NodeIdsList = RemoveElementFromSlice(memList.NodeIdsList, *nodeId)
	}

	return nil
}

// Print that node has failed to stdout and/or the log file/stream
func logFailHelper(logStream *os.File, nodeId *core.NodeID) {
	//LogNodeFail(os.Stdout, nodeId)
	if logStream != nil {
		core.LogNodeFail(logStream, nodeId)
	}
}

// Print that node has joined to stdout and/or the log file/stream
func logJoinHelper(logStream *os.File, nodeId *core.NodeID, status core.NodeStatus) {
	//LogNodeJoin(os.Stdout, nodeId, status)
	if logStream != nil {
		core.LogNodeJoin(logStream, nodeId, status)
	}
}

func RemoveElementFromSlice(mySlice []core.NodeID, element core.NodeID) []core.NodeID {
	var result []core.NodeID

	for _, value := range mySlice {
		if value != element {
			result = append(result, value)
		}
	}

	return result
}

/*
Increments the heartbeat count by 1 for the nodeId entry in the membership list
Additionally updates the Last-Updated-Time in the entry to the current time
*/
func (memList *MembershipList) IncrementHeartbeatCount(id *core.NodeID) {
	memlistRow, exists := memList.MemList[*id]
	if !exists {
		log.Fatal("IncrementHeartbeatCount() - invalid id - does not exist in the map")
	}
	memList.UpdateEntry(id, memlistRow.HeartBeatCount+1, -1, nil)
}

func (memList *MembershipList) ChooseRandomTargets(b int, thisNodeId core.NodeID) []core.NodeID {
	var targets []core.NodeID

	for nodeId, entry := range memList.MemList {
		if nodeId == thisNodeId {
			continue
		}
		if entry.Status == core.ACTIVE || entry.Status == core.SUSPICIOUS {
			targets = append(targets, nodeId)
		}
	}
	targets = ShuffleSlice(targets)

	if len(targets) <= b {
		return targets
	} else {
		return targets[:b]
	}
}

func ShuffleSlice(slice []core.NodeID) []core.NodeID {
	for i := len(slice) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		slice[i], slice[j] = slice[j], slice[i]
	}

	return slice
}

func AreMemListRowsEqual(listRow1 *MembershipListEntry, listRow2 *MembershipListEntry) bool {

	if listRow1.Status == listRow2.Status &&
		listRow1.LastUpdatedTime == listRow2.LastUpdatedTime &&
		listRow1.HeartBeatCount == listRow2.HeartBeatCount {
		return true
	}
	return false
}

func AreMemListsEqual(list1 map[core.NodeID]MembershipListEntry, list2 map[core.NodeID]MembershipListEntry) bool {

	if len(list1) != len(list2) {
		return false
	}

	for key1, val1 := range list1 {

		val2, exists := list2[key1]

		// if corresponding key does exist in other Memlist
		if !exists {
			return false
		}

		// if key exists, want to make sure the vals are the same
		if !AreMemListRowsEqual(&val1, &val2) {
			return false
		}
	}

	return true
}

// getter for membership list
func (memList *MembershipList) GetMembershipList() *MembershipList {
	return memList
}

func LogMembershipList(stream *os.File, memList *MembershipList) {
	_, err := stream.WriteString(fmt.Sprintf(core.MEMBERSHIP_LIST_FMT, utils.GetCurrentTime(), memList.String()))
	if err != nil {
		log.Fatal("LogMembershipList(): Failed to WriteString() to the stream")
	}
}