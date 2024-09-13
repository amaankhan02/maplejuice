package maplejuice

import (
	"cs425_mp4/internal/config"
	"cs425_mp4/internal/core"
	"cs425_mp4/internal/failure_detector"
	"cs425_mp4/internal/sdfs"
	"cs425_mp4/internal/utils"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const MAPLE_JUICE_LEADER_DISPATCHER_WAIT_TIME = 500 * time.Millisecond
const SQL_FILTER_MAPLE_EXE_FILENAME = "maple_SQL_filter"
const SQL_FILTER_JUICE_EXE_FILENAME = "juice_SQL_filter"
const SQL_FILTER_INTERMEDIATE_FILENAME_PREFIX_FMT = "SQL_filter_intermediate_%s_%d" // fmt: (dataset, unix.Nano() time)
const SQL_FILTER_DEST_FILENAME_FMT = "SQL_filter_output_%s"                         // fmt: (dataset)
const SQL_FILTER_NUM_TASKS = 6                                                      // num_maples and num_juices for the SQL filter job

/*
	MapleJuiceManager

Manager of various nodes regarding the maple juice program to run.
Implements FaultTolerable interface

Handles the entire maple juice program. It is the manager of the
SDFSNode, NodeFailureJoinService, and MapleJuiceNode, which are all necessary
for maple juice to run. It also has a parser that parses the command line
input and executes the respective MapleJuiceNode functions.
*/
type MapleJuiceManager struct {
	id                 core.NodeID
	mapleJuiceNode     *MapleJuiceNode
	sdfsNode           *sdfs.SDFSNode
	failureJoinService *failure_detector.NodeFailureJoinService
	parentRootDir      string
	// logFile            *os.File
}

/*
	NewMapleJuiceManager

NOTE: the introducer and leader node are always the same node in this implementation
*/
func NewMapleJuiceManager(
	introducerLeaderVmNum int,
	parentDir string,
	logFile *os.File, // ** REMOVE taking in the logFile, and instead take in the parent directory
	sdfsRootDir string, // TODO: given the parent directory, we should create these directories inside here
	mapleJuiceNodeRootDir string,
	gossipFanout int,
	gossipModeValue failure_detector.GossipModeValue,
	tGossip int64,
) *MapleJuiceManager {
	const GOSSIP_IS_TEST_MODE = false // always false for now, set it true later or remove this if we wanna test it out
	const GOSSIP_TEST_MSG_DROP_RATE = 0
	manager := &MapleJuiceManager{}
	localNodeId, introducerLeaderId, isIntroducerLeader := manager.createLocalAndLeaderNodeID(introducerLeaderVmNum)

	sdfsNode := sdfs.NewSDFSNode(
		*localNodeId,
		*introducerLeaderId,
		isIntroducerLeader,
		logFile,
		sdfsRootDir,
	)
	failureJoinService := failure_detector.NewNodeFailureJoinService(
		*localNodeId,
		gossipFanout,
		isIntroducerLeader,
		*introducerLeaderId,
		logFile,
		gossipModeValue,
		GOSSIP_IS_TEST_MODE,
		GOSSIP_TEST_MSG_DROP_RATE,
		tGossip,
		manager,
	)
	mjNode := NewMapleJuiceNode(
		*localNodeId,
		*introducerLeaderId,
		logFile,
		sdfsNode,
		mapleJuiceNodeRootDir,
		MAPLE_JUICE_LEADER_DISPATCHER_WAIT_TIME,
	)

	manager.id = *localNodeId
	manager.failureJoinService = failureJoinService
	manager.sdfsNode = sdfsNode
	manager.mapleJuiceNode = mjNode
	manager.parentRootDir = parentDir

	return manager
}

func (manager *MapleJuiceManager) Start() {
	// remove and clear the directory if it already exists, and then create it
	_ = utils.DeleteDirAndAllContents(manager.mapleJuiceNode.mjRootDir)
	_ = os.Mkdir(manager.mapleJuiceNode.mjRootDir, 0755)
	// create SDFS root directory (delete it first if it already existed)
	_ = os.RemoveAll(manager.sdfsNode.SdfsDir + "/") // remove and clear the directory if it already exists
	_ = os.Mkdir(manager.sdfsNode.SdfsDir, 0755)

	manager.sdfsNode.Start()
	manager.failureJoinService.JoinGroup()
	manager.mapleJuiceNode.Start()

	// LogMembershipList(os.Stdout, manager.failureJoinService.MembershipList)

	manager.startUserInputLoop()
}

func (manager *MapleJuiceManager) createLocalAndLeaderNodeID(introducerLeaderVmNum int) (*core.NodeID, *core.NodeID, bool) {
	var introducerLeaderId *core.NodeID
	var isIntroducerLeader bool
	vmNum, hostname := utils.GetLocalVMInfo()

	if vmNum == introducerLeaderVmNum { // this node is the leader, so don't need to create separate node ID for leader
		isIntroducerLeader = true
		introducerLeaderId = &core.NodeID{}
	} else { // not the leader, so create the node ID for the leader node
		isIntroducerLeader = false
		introducerHostname := utils.GetHostname(introducerLeaderVmNum)
		introducerLeaderId = core.NewNodeID(
			utils.GetIP(introducerHostname),
			utils.GetGossipPort(introducerLeaderVmNum),
			utils.GetSDFSPort(introducerLeaderVmNum),
			utils.GetMapleJuicePort(introducerLeaderVmNum),
			true, // we're creating the introducer/leader id so mark it as true
			introducerHostname,
		)
	}
	localNodeId := core.NewNodeID(
		utils.GetIP(hostname),
		utils.GetGossipPort(vmNum),
		utils.GetSDFSPort(vmNum),
		utils.GetMapleJuicePort(vmNum),
		isIntroducerLeader,
		hostname,
	)
	if isIntroducerLeader {
		introducerLeaderId = localNodeId
	}

	return localNodeId, introducerLeaderId, isIntroducerLeader
}

func (manager *MapleJuiceManager) startUserInputLoop() {
	for {
		fmt.Println()
		input, err := utils.ReadUserInput()
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "Failed to read user input")
		}

		cmdArgsInput := strings.Fields(input) // split user input up based on spaces
		if shouldExit := manager.executeUserInput(cmdArgsInput); shouldExit {
			break
		}
	}
}

// executeUserInput returns true if the user input was to leave the cluster, therefore exiting the program
func (manager *MapleJuiceManager) executeUserInput(userInput []string) bool {
	if len(userInput) == 0 {
		return false
	}

	switch strings.ToLower(userInput[0]) {
	case "enable":
		if len(userInput) != 2 {
			fmt.Println("Invalid number of arguments for enable command")
			return false
		}
		if strings.ToLower(userInput[1]) == "suspicion" {
			ok := manager.failureJoinService.TryUpdateGossipMode(failure_detector.GossipMode{
				Mode:          failure_detector.GOSSIP_SUSPICION,
				VersionNumber: manager.failureJoinService.CurrentGossipMode.VersionNumber + 1},
			)
			if !ok {
				fmt.Println("Suspicion mode is already enabled! Nothing to change...")
			}
		} else {
			fmt.Println("Invalid command")
			return false
		}
	case "disable":
		if len(userInput) != 2 {
			fmt.Println("Invalid number of arguments for disable command")
			return false
		}
		if strings.ToLower(userInput[1]) == "suspicion" {
			ok := manager.failureJoinService.TryUpdateGossipMode(failure_detector.GossipMode{
				Mode:          failure_detector.GOSSIP_NORMAL,
				VersionNumber: manager.failureJoinService.CurrentGossipMode.VersionNumber + 1},
			)
			if !ok {
				fmt.Println("Suspicion mode is already disabled! Nothing to change...")
			}
		} else {
			fmt.Println("Invalid command")
			return false
		}
	case "mode":
		fmt.Printf("Current maplejuice mode: %s\n\n", manager.failureJoinService.CurrentGossipMode.Mode.String())
	case "list_mem":
		failure_detector.LogMembershipList(os.Stdout, manager.failureJoinService.MembershipList)
	case "list_self":
		fmt.Println(manager.failureJoinService.Id.ToStringForGossipLogger())
	case "leave":
		manager.failureJoinService.LeaveGroup()
		// NOTE: call respective leave functions for sdfs and maple juice - future improvement
		return true
	case "get":
		if len(userInput) != 3 {
			fmt.Println("Invalid usage. Expected usage: get [sdfs_filename] [local_filename]")
			return false
		}
		sdfsFilename := userInput[1]
		localFilename := userInput[2]
		manager.sdfsNode.PerformGet(sdfsFilename, localFilename)
	case "put":
		if len(userInput) != 3 {
			fmt.Println("Invalid usage. Expected usage: put [local_filename] [sdfs_filename]")
			return false
		}
		localFilename := userInput[1]
		sdfsFilename := userInput[2]
		manager.sdfsNode.PerformPut(localFilename, sdfsFilename)
	case "ls":
		if len(userInput) != 2 {
			fmt.Println("Invalid usage. Expected usage: ls [sdfs_filename]")
			return false
		}
		manager.sdfsNode.PerformLs(userInput[1])
	case "store":
		if len(userInput) != 1 {
			fmt.Println("Invalid usage. Expected usage: store")
			return false
		}
		manager.sdfsNode.PerformStore()
	case "multiread":
		if len(userInput) < 4 {
			fmt.Println("Invalid usage. Expected usage: multiread [sdfs_filename] [local_filename] [VMi], ..., [VMj]")
			return false
		}
		manager.sdfsNode.PerformMultiRead(userInput[1], userInput[2], userInput[3:])
	case "acknowledgement":
		if len(userInput) > 1 {
			fmt.Println("Invalid usage. Expected usage: acknowledgement")
			return false
		}
		manager.sdfsNode.PerformAcknowledgementsPrint()
	case "delete":
		if len(userInput) != 2 {
			fmt.Println("Invalid usage. Expected usage: delete [sdfs_filename]")
			return false
		}
		manager.sdfsNode.PerformDelete(userInput[1])
	case "maple":
		manager.parseAndExecuteMapleInput(userInput)
	case "juice":
		manager.parseAndExecuteJuiceInput(userInput)
	case "select":
		manager.parseAndExecuteSqlQuery(userInput)
	case "test1":
		if len(userInput) != 4 {
			fmt.Println("Invalid usage. Expected usage: test1 [x] [num_workers] [dataset]")
			return false
		}
		x := userInput[1]
		numWorkers, _ := strconv.Atoi(userInput[2])
		dataset := userInput[3]
		manager.handleTest1(x, numWorkers, dataset)
	}
	return false
}

func (manager *MapleJuiceManager) handleTest1(x string, numWorkers int, dataset string) {
	/*
		maple maple_demo_phase1 3 p1 traffic
		juice juice_demo_phase1 3 p1 p1_out delete_input=0

		maple maple_demo_phase2 3 p2 p1_ou
		juice juice_demo_phase2 3 p2 p2_out delete_input=0
	*/
	m1FilePath, _ := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, "maple_demo_phase1"))
	j1FilePath, _ := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, "juice_demo_phase1"))
	m2FilePath, _ := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, "maple_demo_phase2"))
	j2FilePath, _ := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, "juice_demo_phase2"))

	m1Exe := MapleJuiceExeFile{
		ExeFilePath:       m1FilePath,
		SqlAdditionalInfo: x,
	}
	j1Exe := MapleJuiceExeFile{
		ExeFilePath: j1FilePath,
	}
	m2Exe := MapleJuiceExeFile{
		ExeFilePath: m2FilePath,
	}
	j2Exe := MapleJuiceExeFile{
		ExeFilePath: j2FilePath,
	}

	manager.mapleJuiceNode.SubmitMapleJob(
		m1Exe,
		numWorkers,
		"p1",
		dataset,
	)
	time.Sleep(1 * time.Second)
	manager.mapleJuiceNode.SubmitJuiceJob(
		j1Exe,
		numWorkers,
		"p1",
		"p1_out.result",
		false,
		HASH_PARTITIONING,
	)
	time.Sleep(1 * time.Second)
	manager.mapleJuiceNode.SubmitMapleJob(
		m2Exe,
		numWorkers,
		"p2",
		"p1_out",
	)
	time.Sleep(1 * time.Second)
	manager.mapleJuiceNode.SubmitJuiceJob(
		j2Exe,
		numWorkers,
		"p2",
		"test1_result",
		false,
		HASH_PARTITIONING,
	)

	fmt.Println("ANSWER IN test1_result")
}

func (manager *MapleJuiceManager) parseAndExecuteSqlQuery(userInput []string) {
	// FILTER: SELECT ALL FROM DATASET WHERE <REGEX>
	// JOIN: SELECT ALL FROM D1, D2 WHERE D1.FIELD = D2.FIELD
	num_data_sets := GetNumDatasets(userInput)

	if num_data_sets == 1 { // FILTER
		dataset, regex := parseSqlFilterQuery(userInput)
		manager.executeSqlFilter(dataset, regex)
	} else if num_data_sets == 2 { // JOIN
		d1, d2, field1, field2 := parseSqlJoinQuery(userInput)
		manager.executeSqlJoin(d1, d2, field1, field2)
	} else { //INCORRECT
		fmt.Println("Invalid SQL Query")
	}
}

func (manager *MapleJuiceManager) executeSqlFilter(dataset string, regex string) {
	// dataset = src directory in sdfs
	// regex = regex to match --> put under SQL additional info

	// get full path to the maple_exe file and juice_exe file
	mapleExeFilePath, err1 := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, SQL_FILTER_MAPLE_EXE_FILENAME))
	if err1 != nil {
		fmt.Println("Unable to parse maple_exe name")
		return
	}
	mapleExe := MapleJuiceExeFile{
		ExeFilePath:       mapleExeFilePath,
		SqlAdditionalInfo: regex,
	}

	juiceExeFilePath, err2 := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, SQL_FILTER_JUICE_EXE_FILENAME))
	if err2 != nil {
		fmt.Println("Unable to parse juice_exe name")
		return
	}
	juiceExe := MapleJuiceExeFile{
		ExeFilePath: juiceExeFilePath,
	}
	sdfsIntermediateFileName := fmt.Sprintf(SQL_FILTER_INTERMEDIATE_FILENAME_PREFIX_FMT, dataset, time.Now().Unix())
	sdfsDestFileName := fmt.Sprintf(SQL_FILTER_DEST_FILENAME_FMT, dataset)

	fmt.Printf("Submitting MapleJuice job for SQL filter.\nSDFS Destination File: %s\nSDFS Intermediate Filename Prefix: %s\n",
		sdfsDestFileName, sdfsIntermediateFileName)

	manager.mapleJuiceNode.SubmitMapleJob(
		mapleExe,
		SQL_FILTER_NUM_TASKS,
		sdfsIntermediateFileName,
		dataset,
	)
	time.Sleep(1 * time.Second) // give it enough time for maple to be submitted
	manager.mapleJuiceNode.SubmitJuiceJob(
		juiceExe,
		SQL_FILTER_NUM_TASKS,
		sdfsIntermediateFileName,
		sdfsDestFileName,
		false,
		HASH_PARTITIONING,
	)
	time.Sleep(1 * time.Second)

	fmt.Println("ANSWER IN ", sdfsDestFileName)
}

func parseSqlFilterQuery(userInput []string) (string, string) {
	dataset := userInput[3]
	regex := userInput[5]
	// Check if the string starts and ends with double quotes
	if len(regex) >= 2 && regex[0] == '"' && regex[len(regex)-1] == '"' {
		// Remove the first and last character (the double quotes)
		regex = regex[1 : len(regex)-1]
	}
	return dataset, regex
}

func GetNumDatasets(userInput []string) int {
	num_data_sets := 0
	start_counting := false

	contains_FROM, contains_WHERE := doesQueryHasFROMandWHERE(userInput)

	if !contains_FROM || !contains_WHERE {
		fmt.Println("Incorrect SQL Query, does not contains FROM or WHERE")
		return -1
	}

	// count number of words between FROM and WHERE (because this is number of datasets)
	for i := 0; i < len(userInput); i++ {
		word := userInput[i]

		if word == "FROM" {
			start_counting = true
			continue
		}

		if word == "WHERE" { // no longer looking at the number of datasets
			break
		}

		if start_counting {
			num_data_sets += 1
		}
	}

	return num_data_sets
}

func doesQueryHasFROMandWHERE(userInput []string) (bool, bool) {
	contains_FROM := false
	contains_WHERE := false
	for _, word := range userInput {
		if word == "FROM" {
			contains_FROM = true
		} else if word == "WHERE" {
			contains_WHERE = true
		}
	}

	return contains_FROM, contains_WHERE
}

func (manager *MapleJuiceManager) parseAndExecuteJuiceInput(userInput []string) {
	juiceExeFile := userInput[1]
	juiceExeFilePath, err1 := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, juiceExeFile))
	if err1 != nil {
		fmt.Println("Unable to parse juice_exe name")
		return
	}
	juiceExe := MapleJuiceExeFile{
		ExeFilePath: juiceExeFilePath,
	}
	numJuices, parse_err := strconv.Atoi(userInput[2])
	if parse_err != nil {
		fmt.Println("Number of Juices parameter is invalid number!")
		return
	}
	sdfsIntermediateFilenamePrefix := userInput[3]
	sdfsDestFilename := userInput[4]
	var shouldDelete bool
	shouldDelStrings := strings.Split(userInput[5], "=")
	if shouldDelStrings[0] != "delete_input" {
		fmt.Println("Invalid input for delete_input parameter. Must be delete_input=0 or delete_input=1")
		return
	}
	if shouldDelStrings[1] == "0" {
		shouldDelete = false
	} else if shouldDelStrings[1] == "1" {
		shouldDelete = true
	} else {
		fmt.Println("Invalid input for delete_input parameter")
		return
	}
	juicePartitionScheme := HASH_PARTITIONING

	manager.mapleJuiceNode.SubmitJuiceJob(
		juiceExe,
		numJuices,
		sdfsIntermediateFilenamePrefix,
		sdfsDestFilename,
		shouldDelete,
		juicePartitionScheme,
	)
}

func (manager *MapleJuiceManager) parseAndExecuteMapleInput(userInput []string) {
	mapleExeFileName := userInput[1] // just type in "maple_word_count"

	// join with "bin" directory, and then get absolute path
	mapleExeFilePath, err1 := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, mapleExeFileName))
	if err1 != nil {
		fmt.Println("Unable to parse maple_exe name")
		return
	}
	mapleExe := MapleJuiceExeFile{
		ExeFilePath: mapleExeFilePath,
	}
	numMaples, parse_err := strconv.Atoi(userInput[2])
	if parse_err != nil {
		fmt.Println("Number of Maples parameter is invalid number!")
		return
	}
	sdfsIntermediateFilenamePrefix := userInput[3]
	sdfsSrcDirectory := userInput[4]

	manager.mapleJuiceNode.SubmitMapleJob(mapleExe, numMaples, sdfsIntermediateFilenamePrefix, sdfsSrcDirectory)
}

func parseSqlJoinQuery(userInput []string) (string, string, string, string) {
	d1 := userInput[3]
	d2 := userInput[5]
	field1 := userInput[7]
	field2 := userInput[9]

	return d1, d2, field1, field2
}

func (manager *MapleJuiceManager) executeSqlJoin(d1 string, d2 string, f1 string, f2 string) {
	// dataset = src directory in sdfs
	// regex = regex to match --> put under SQL additional info

	// get full path to the maple_exe file and juice_exe file
	mapleExeFilePath, err1 := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, SQL_FILTER_MAPLE_EXE_FILENAME))
	if err1 != nil {
		fmt.Println("Unable to parse maple_exe name")
		return
	}
	mapleExe := MapleJuiceExeFile{
		ExeFilePath:       mapleExeFilePath,
		SqlAdditionalInfo: f1,
	}
	mapleExe2 := MapleJuiceExeFile{
		ExeFilePath:       mapleExeFilePath,
		SqlAdditionalInfo: f2,
	}

	juiceExeFilePath, err2 := filepath.Abs(filepath.Join(config.EXE_FILES_FOLDER, SQL_FILTER_JUICE_EXE_FILENAME))
	if err2 != nil {
		fmt.Println("Unable to parse juice_exe name")
		return
	}
	juiceExe := MapleJuiceExeFile{
		ExeFilePath: juiceExeFilePath,
	}
	sdfsIntermediateFileName := fmt.Sprintf(SQL_FILTER_INTERMEDIATE_FILENAME_PREFIX_FMT, d1, time.Now().Unix())
	sdfsDestFileName := fmt.Sprintf(SQL_FILTER_DEST_FILENAME_FMT, d1)

	fmt.Printf("Submitting MapleJuice job for SQL filter.\nSDFS Destination File: %s\nSDFS Intermediate Filename Prefix: %s\n",
		sdfsDestFileName, sdfsIntermediateFileName)

	manager.mapleJuiceNode.SubmitMapleJob(
		mapleExe,
		SQL_FILTER_NUM_TASKS,
		sdfsIntermediateFileName,
		d1,
	)
	time.Sleep(1 * time.Second) // give it enough time for maple to be submitted
	manager.mapleJuiceNode.SubmitJuiceJob(
		juiceExe,
		SQL_FILTER_NUM_TASKS,
		sdfsIntermediateFileName,
		sdfsDestFileName,
		false,
		HASH_PARTITIONING,
	)
	time.Sleep(1 * time.Second)
	manager.mapleJuiceNode.SubmitMapleJob(
		mapleExe2,
		SQL_FILTER_NUM_TASKS,
		sdfsIntermediateFileName,
		d2,
	)
	time.Sleep(1 * time.Second) // give it enough time for maple to be submitted
	manager.mapleJuiceNode.SubmitJuiceJob(
		juiceExe,
		SQL_FILTER_NUM_TASKS,
		sdfsIntermediateFileName,
		sdfsDestFileName,
		false,
		HASH_PARTITIONING,
	)
	time.Sleep(1 * time.Second)

	fmt.Println("ANSWER IN ", sdfsDestFileName)
}

func (manager *MapleJuiceManager) HandleNodeFailure(info failure_detector.FailureDetectionInfo) {
	// only handle if we are the leader. cuz otherwise the gossip will eventually send it to the leader
	if manager.sdfsNode.IsLeader {
		manager.sdfsNode.LeaderService.IndicateNodeFailed(info.FailedNodeId)
	}
	if manager.mapleJuiceNode.isLeader {
		manager.mapleJuiceNode.leaderService.IndicateNodeFailed(info.FailedNodeId)
	}
}

func (manager *MapleJuiceManager) HandleNodeJoin(info failure_detector.NodeJoinInfo) {
	// if a node joined our membership list, i need to reflect that in leaderService.AvailableWorkerNodes
	if manager.sdfsNode.IsLeader {
		manager.sdfsNode.LeaderService.AddNewActiveNode(info.JoinedNodeId)
	}
	if manager.mapleJuiceNode.isLeader {
		manager.mapleJuiceNode.leaderService.AddNewAvailableWorkerNode(info.JoinedNodeId)
	}
}
