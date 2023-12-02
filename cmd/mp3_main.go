package main


import (
	"cs425_mp4/internal/config"
	"cs425_mp4/internal/maplejuice"
	"cs425_mp4/internal/utils"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

var sdfsNode *maplejuice.SDFSNode

var logFileName *string
var logFile *os.File
var isTestMode bool
var msgDropRate *int
var tgossip *int64

var finalTGossip int64 // nanoseconds -- pass in as parameter

func main() {

	// create SDFS root directory
	_ = os.RemoveAll(config.SDFS_ROOT_DIR + "/") // remove and clear the directory if it already exists
	_ = os.Mkdir(config.SDFS_ROOT_DIR, 0755)

	ParseArguments()
	Init()
	defer logFile.Close()

	fmt.Printf("TGossip: %dms\n", *tgossip)
	if isTestMode {
		fmt.Printf("Booting up in TEST mode with %d percent message drop rate\n", *msgDropRate)
	}

	sdfsNode.Start()

	for {
		fmt.Println()
		input, err := utils.ReadUserInput()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Failed to read user input")
		}

		cmdArgsInput := strings.Fields(input) // split user input up based on spaces
		if shouldExit := ExecuteUserInput(cmdArgsInput); shouldExit {
			break
		}
	}
}

func ParseArguments() {
	var err error
	logFileName = flag.String("f", "", "Filename of the logfile to write to")
	msgDropRate = flag.Int("t", -1, "Pass in a float between 0 and 100 representing the message drop rate as a percent if "+
		"you wish to boot up in TEST mode. Otherwise passing in nothing will NOT boot up program in TEST mode")
	tgossip = flag.Int64("g", config.DEFAULT_T_GOSSIP, "T GOSSIP in milliseconds (1000 ms = 1s)")
	flag.Parse()

	finalTGossip = *tgossip * 1e6 // convert to ns

	// creates the file if it doesn't exist, if it does, then truncates it and opens it
	logFile, err = os.Create(*logFileName)
	if err != nil {
		log.Fatalf("Failed to open file %s", *logFileName)
	}

	if *msgDropRate == -1 { // -1.0 means it's NOT booting up in test mode
		isTestMode = false
		*msgDropRate = 0 // turn to 0 so that the program doesn't drop anything
	} else {
		isTestMode = true
	}
}

func Init() {
	RegisterStructsForSerialization()
	var introducerLeaderId *maplejuice.NodeID
	var isIntroducerLeader bool
	vmNum, hostname := utils.GetLocalVMInfo()

	// the introducer and leader node are always the same node in this implementation
	if vmNum == config.INTRODUCER_LEADER_VM {
		isIntroducerLeader = true
		introducerLeaderId = &maplejuice.NodeID{}
	} else {
		isIntroducerLeader = false
		introducerHostname := utils.GetHostname(config.INTRODUCER_LEADER_VM)
		introducerLeaderId = maplejuice.NewNodeID(
			utils.GetIP(introducerHostname),
			utils.GetGossipPort(config.INTRODUCER_LEADER_VM),
			utils.GetSDFSPort(config.INTRODUCER_LEADER_VM),
			true, // we're creating the introducer/leader id so mark it as true
			introducerHostname,
		)
	}
	thisNodeId := maplejuice.NewNodeID(utils.GetIP(hostname), utils.GetGossipPort(vmNum), utils.GetSDFSPort(vmNum), isIntroducerLeader, hostname)

	sdfsNode = maplejuice.NewSDFSNode(*thisNodeId,
		*introducerLeaderId,
		isIntroducerLeader,
		logFile,
		config.SDFS_ROOT_DIR,
		isTestMode,
		*msgDropRate,
		finalTGossip,
	)
}

func RegisterStructsForSerialization() {
	gob.Register(&maplejuice.MembershipList{})
	gob.Register(&maplejuice.MembershipListEntry{})
	gob.Register(&maplejuice.ShardMetaData{})
	gob.Register(&maplejuice.NodeID{})
	gob.Register(&maplejuice.Shard{})
	gob.Register(&maplejuice.GetInfoRequest{})
	gob.Register(&maplejuice.GetInfoResponse{})
	gob.Register(&maplejuice.PutInfoResponse{})
	gob.Register(&maplejuice.PutInfoRequest{})
	gob.Register(&maplejuice.GetDataRequest{})
	gob.Register(&maplejuice.GetDataResponse{})
	gob.Register(&maplejuice.PutDataRequest{})
	gob.Register(&maplejuice.Ack{})
	gob.Register(&maplejuice.DeleteInfoRequest{})
	gob.Register(&maplejuice.DeleteInfoResponse{})
	gob.Register(&maplejuice.DeleteDataRequest{})
	gob.Register(&maplejuice.DeleteDataRequest{})
}

// Executes the user input
// Returns true if the userInput was "leave", indicating to exit out of program
func ExecuteUserInput(userInput []string) bool {
	if len(userInput) == 0 {
		return false
	}
	gossipNode := sdfsNode.ThisGossipNode

	if userInput[0] == "enable" && userInput[1] == "suspicion" {
		ok := gossipNode.TryUpdateGossipMode(maplejuice.GossipMode{
			maplejuice.GOSSIP_SUSPICION,
			gossipNode.CurrentGossipMode.VersionNumber + 1},
		)
		if !ok {
			fmt.Println("Suspicion mode is already enabled! Nothing to change...")
		}
	} else if userInput[0] == "disable" && userInput[1] == "suspicion" {
		ok := gossipNode.TryUpdateGossipMode(maplejuice.GossipMode{
			maplejuice.GOSSIP_NORMAL,
			gossipNode.CurrentGossipMode.VersionNumber + 1},
		)
		if !ok {
			fmt.Println("Suspicion mode is already disabled! Nothing to change...")
		}
	} else if userInput[0] == "mode" {
		fmt.Printf("Current maplejuice mode: %s\n\n", gossipNode.CurrentGossipMode.Mode.String())
	} else if userInput[0] == "list_mem" {
		maplejuice.LogMembershipList(os.Stdout, gossipNode.MembershipList)
	} else if userInput[0] == "list_self" {
		fmt.Println(gossipNode.Id.ToStringForGossipLogger())
	} else if userInput[0] == "leave" {
		gossipNode.LeaveGroup()
		return true
	} else if strings.ToLower(userInput[0]) == "get" {
		if len(userInput) != 3 {
			fmt.Println("Invalid usage. Expected usage: get [sdfs_filename] [local_filename]")
			return false
		}
		sdfsFilename := userInput[1]
		localFilename := userInput[2]
		sdfsNode.PerformGet(sdfsFilename, localFilename)
	} else if strings.ToLower(userInput[0]) == "put" {
		if len(userInput) != 3 {
			fmt.Println("Invalid usage. Expected usage: put [local_filename] [sdfs_filename]")
			return false
		}
		localFilename := userInput[1]
		sdfsFilename := userInput[2]
		sdfsNode.PerformPut(localFilename, sdfsFilename)
	} else if strings.ToLower(userInput[0]) == "ls" {
		if len(userInput) != 2 {
			fmt.Println("Invalid usage. Expected usage: ls [sdfs_filename]")
			return false
		}
		sdfsNode.PerformLs(userInput[1])
	} else if strings.ToLower(userInput[0]) == "store" {
		if len(userInput) != 1 {
			fmt.Println("Invalid usage. Expected usage: store")
			return false
		}
		sdfsNode.PerformStore()
	} else if strings.ToLower(userInput[0]) == "multiread" {
		if len(userInput) < 4 {
			fmt.Println("Invalid usage. Expected usage: multiread [sdfs_filename] [local_filename] [VMi], ..., [VMj]")
			return false
		}
		sdfsNode.PerformMultiRead(userInput[1], userInput[2], userInput[3:])
	} else if strings.ToLower(userInput[0]) == "acknowledgement" {
		if len(userInput) > 1 {
			fmt.Println("Invalid usage. Expected usage: acknowledgement")
			return false
		}
		sdfsNode.PerformAcknowledgementsPrint()
	} else if strings.ToLower(userInput[0]) == "delete" {
		if len(userInput) != 2 {
			fmt.Println("Invalid usage. Expected usage: delete [sdfs_filename]")
			return false
		}
		sdfsNode.PerformDelete(userInput[1])
	}

	return false
}
