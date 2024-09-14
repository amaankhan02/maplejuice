package main

import (
	"cs425_mp4/internal/config"
	"cs425_mp4/internal/core"
	"cs425_mp4/internal/failure_detector"
	"cs425_mp4/internal/maplejuice"
	"cs425_mp4/internal/sdfs"
	"cs425_mp4/internal/utils"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

// var logFileName *string
var tgossip *int64
var msgDropRate *int
var isDataCollectMode *bool
var testName *string

var failureJoinServiceTestingInfo *failure_detector.NodeFailureJoinServiceTestingParams
var finalTGossip int64
var logFile *os.File
var sdfsRootDir string
var mapleJuiceRootDir string

func main() {
	ParseArguments()
	InitializeDirectoryAndFiles()
	RegisterStructsForSerialization()
	defer func(logFile *os.File) {
		_ = logFile.Close()
	}(logFile)

	PrintGreeting()

	mjManager := maplejuice.NewMapleJuiceManager(
		config.INTRODUCER_LEADER_VM,
		config.APP_ROOT_DIR,
		logFile,
		sdfsRootDir,
		mapleJuiceRootDir,
		failure_detector.GOSSIP_NORMAL,
		*failureJoinServiceTestingInfo,
		finalTGossip,
	)

	mjManager.Start()
	fmt.Printf("----------------------------------------------------------------\n")
}

func PrintGreeting() {
	fmt.Printf("----------------------------------------------------------------\n")
	vmNum, hostname := utils.GetLocalVMInfo()
	if vmNum == config.INTRODUCER_LEADER_VM {
		fmt.Printf("Starting MapleJuice on %s (Introducer & Leader)\n", hostname)
	} else {
		fmt.Printf("Starting MapleJuice on %s\n", hostname)
	}

	if *isDataCollectMode {
		fmt.Println("Running in Data Collection Mode")
		fmt.Println("\tCollecting data for test type " + *testName)
		fmt.Println("\tMessage drop rate: " + fmt.Sprintf("%d", *msgDropRate) + "%")
		fmt.Println("\tGossip time: " + fmt.Sprintf("%d", *tgossip) + "ms")
	}

	fmt.Printf("----------------------------------------------------------------\n")
}
func InitializeDirectoryAndFiles() {
	// create app root dir
	if _, err := os.Stat(config.APP_ROOT_DIR); errors.Is(err, os.ErrNotExist) {
		err = os.Mkdir(config.APP_ROOT_DIR, os.ModePerm)
		if err != nil {
			log.Fatalf("Failed to create APP_ROOT_DIR %s", config.APP_ROOT_DIR)
		}
	}

	// create nested directories
	var fileerr error
	mapleJuiceRootDir = filepath.Join(config.APP_ROOT_DIR, config.MAPLE_JUICE_ROOT_DIR)
	sdfsRootDir = filepath.Join(config.APP_ROOT_DIR, config.SDFS_ROOT_DIR)

	// creates the debug file if it doesn't exist, if it does, then truncates it and opens it
	logFile, fileerr = os.Create(filepath.Join(config.APP_ROOT_DIR, config.DEBUG_OUTPUT_FILENAME))
	if fileerr != nil {
		log.Fatalf("Failed to create file %s", filepath.Join(config.APP_ROOT_DIR, config.DEBUG_OUTPUT_FILENAME))
	}
}

func ParseArguments() {
	// parse command line arguments
	tgossip = flag.Int64("g", config.DEFAULT_T_GOSSIP, "T GOSSIP in milliseconds (1000 ms = 1s)")
	isDataCollectMode = flag.Bool("d", false, "Run in data collection mode")
	msgDropRate = flag.Int("r", 0, "Message drop rate as a percentage (0-100)")
	testName = flag.String("n", "", "Test name (string)")
	flag.Parse()

	// finalize values
	finalTGossip = *tgossip * 1e6 // convert to ns
	failureJoinServiceTestingInfo = &failure_detector.NodeFailureJoinServiceTestingParams{
		TestType:           *testName,
		MsgDropRate:        *msgDropRate,
		IsTestMode:         *isDataCollectMode,
		AppendToDataFile:   true, // currently has no effect since it's not used
		DataOutputFileName: config.GOSSIP_DATA_ANALYTICS_OUTPUT_FILENAME,
	}
}

func RegisterStructsForSerialization() {
	gob.Register(&failure_detector.MembershipList{})
	gob.Register(&failure_detector.MembershipListEntry{})
	gob.Register(&sdfs.ShardMetaData{})
	gob.Register(&core.NodeID{})
	gob.Register(&sdfs.Shard{})
	gob.Register(&sdfs.GetInfoRequest{})
	gob.Register(&sdfs.GetInfoResponse{})
	gob.Register(&sdfs.PutInfoResponse{})
	gob.Register(&sdfs.PutInfoRequest{})
	gob.Register(&sdfs.GetDataRequest{})
	gob.Register(&sdfs.GetDataResponse{})
	gob.Register(&sdfs.PutDataRequest{})
	gob.Register(&sdfs.Ack{})
	gob.Register(&sdfs.DeleteInfoRequest{})
	gob.Register(&sdfs.DeleteInfoResponse{})
	gob.Register(&sdfs.DeleteDataRequest{})
	gob.Register(&sdfs.DeleteDataRequest{})
	gob.Register(&maplejuice.MapleJuiceNetworkMessage{})
}
