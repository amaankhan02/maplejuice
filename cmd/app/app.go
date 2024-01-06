package main

import (
	"cs425_mp4/internal/config"
	"cs425_mp4/internal/core"
	"cs425_mp4/internal/failure_detector"
	"cs425_mp4/internal/maplejuice"
	"cs425_mp4/internal/sdfs"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
)

var logFileName *string
var logFile *os.File
var tgossip *int64
var finalTGossip int64

func main() {
	ParseArguments()
	RegisterStructsForSerialization()
	defer func(logFile *os.File) {
		_ = logFile.Close()
	}(logFile)
	fmt.Printf("TGossip: %dms\n", *tgossip)

	mjManager := maplejuice.NewMapleJuiceManager(
		config.INTRODUCER_LEADER_VM,
		logFile,
		config.SDFS_ROOT_DIR,
		config.MAPLE_JUICE_ROOT_DIR,
		config.FANOUT,
		failure_detector.GOSSIP_NORMAL,
		finalTGossip,
	)

	mjManager.Start()
}

func ParseArguments() {
	var err error
	logFileName = flag.String("f", "test.log", "Filename of the logfile to write to")
	tgossip = flag.Int64("g", config.DEFAULT_T_GOSSIP, "T GOSSIP in milliseconds (1000 ms = 1s)")
	flag.Parse()

	finalTGossip = *tgossip * 1e6 // convert to ns

	// creates the file if it doesn't exist, if it does, then truncates it and opens it
	logFile, err = os.Create(*logFileName)
	if err != nil {
		log.Fatalf("Failed to open file %s", *logFileName)
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
