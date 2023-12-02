package config

import "time"

const (
	KILOBYTE = 1 << 10
	MEGABYTE = KILOBYTE << 10
	GIGABYTE = MEGABYTE << 10
)

// General configuration variables
const (
	HOSTNAME_FORMAT = "fa23-cs425-19%02d.cs.illinois.edu"
	VM_NUMBER_START = 13 // index from the hostname representing the start of the vm number
	VM_NUM_END      = 15 // index from the hostname representing the end of the vm number (exclusive)
)

// Gossip Failure Detection Configuration Variables
const (
	GOSSIP_PORT_FORMAT       = "81%02d" // 8101, 8102, ... 8110 - based on the hostname number
	INTRODUCER_LEADER_VM int = 1        // the introducer & leader is vm 1

	T_FAIL_NORMAL     time.Duration = 5500 * time.Millisecond // 2 seconds	- T_FAIL used in NORMAL mode
	T_FAIL_SUSPICIOUS time.Duration = 1500 * time.Millisecond // T_FAIL used in SUSPICIOUS mode, (amt of time in suspicious mode allowable)
	T_SUSPICIOUS      time.Duration = 3 * time.Second
	T_CLEANUP         time.Duration = 3 * time.Second
	T_PERIODIC_CHECK  time.Duration = 1 * time.Second // check every 0.5 seconds
	DEFAULT_T_GOSSIP  int64         = 1000            // milliseconds - default value if not passed in as an argument
	FANOUT            int           = 4               // number of maplejuice targets to send heartbeat to per maplejuice period
)

// MP3 Configuration Variables
const (
	SDFS_TCP_PORT_FORMAT        = "82%02d" // 8201, 8202, ... 8210 - based on the hostname number
	SDFS_ROOT_DIR        string = "sdfs_data"

	SHARD_LOCAL_FILENAME_FORMAT                  = "%03d-%s" // "shard_index-dash-sdfs_filename", e.g.: "0002-myfile.txt"
	SHARD_SIZE                     int64         = 200 * MEGABYTE
	REPLICA_COUNT                                = 4
	T_DISPATCHER_WAIT              time.Duration = 100 * time.Millisecond
	MAX_NUM_CONCURRENT_READS       int           = 2
	MAX_NUM_CONCURRENT_WRITES      int           = 1
	MAX_NUM_CONSECUTIVE_OPERATIONS               = 4
)

// MP4 Configuration Variables
const (
	MAPLE_JUICE_PORT_FORMAT     = "83%02d" // 8301, 8302, ... 8310 - based on the hostname number
	LINES_PER_MAPLE_EXE     int = 20
)
