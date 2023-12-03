package main

import (
	"cs425_mp4/internal/config"
	"cs425_mp4/internal/maplejuice"
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
		maplejuice.GOSSIP_NORMAL,
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
	gob.Register(&maplejuice.MapleJuiceNetworkMessage{})
}
