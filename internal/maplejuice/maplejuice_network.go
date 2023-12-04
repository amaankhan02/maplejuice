package maplejuice

import (
	"bufio"
	"bytes"
	"cs425_mp4/internal/tcp_net"
	"cs425_mp4/internal/utils"
	"encoding/gob"
	"fmt"
	"log"
	"net"
)

type MapleJuiceNetworkMessageType byte
type JuicePartitionType byte

const (
	MAPLE_TASK_REQUEST  MapleJuiceNetworkMessageType = 0x1
	MAPLE_TASK_RESPONSE MapleJuiceNetworkMessageType = 0x2
	MAPLE_JOB_REQUEST   MapleJuiceNetworkMessageType = 0x3
	MAPLE_JOB_RESPONSE  MapleJuiceNetworkMessageType = 0x4

	JUICE_TASK_REQUEST  MapleJuiceNetworkMessageType = 0x5
	JUICE_TASK_RESPONSE MapleJuiceNetworkMessageType = 0x6
	JUICE_JOB_REQUEST   MapleJuiceNetworkMessageType = 0x7
	JUICE_JOB_RESPONSE  MapleJuiceNetworkMessageType = 0x8
)

const (
	NOT_APPLICABLE     JuicePartitionType = 0x0
	HASH_PARTITIONING  JuicePartitionType = 0x1
	RANGE_PARTITIONING JuicePartitionType = 0x2
)

type MapleJuiceNetworkMessage struct {
	MsgType                        MapleJuiceNetworkMessageType
	JuicePartitionScheme           JuicePartitionType
	NumTasks                       int // number of maples or juice tasks (depending on the type)
	ExeFile                        MapleJuiceExeFile
	SdfsIntermediateFilenamePrefix string // prefix of the intermediate filenames (output of Maple, input of Juice)
	SdfsSrcDirectory               string // location of input files for Maple
	SdfsDestFilename               string // filename location for output of Juice step where all key-value pairs are appended
	ShouldDeleteJuiceInput         bool   // if true, after the juice phase is done, the sdfs intermediate files will be deleted (inputs for the juice phase)
	CurrTaskIdx                    int
	ClientId                       NodeID
	TaskOutputFileSize             int64
	ClientJobId                    int      // id that the client created for the job it submitted
	Keys                           []string // used for juice tasks (to know what keys to operate on)
}

func SendMapleJobResponse(conn net.Conn, clientJobId int) {
	msg := MapleJuiceNetworkMessage{
		MsgType:     MAPLE_JOB_RESPONSE,
		ClientJobId: clientJobId,
	}
	SendMapleJuiceNetworkMessage(conn, &msg)
}

func SendJuiceJobResponse(conn net.Conn, clientJobId int) {
	msg := MapleJuiceNetworkMessage{
		MsgType:     JUICE_JOB_RESPONSE,
		ClientJobId: clientJobId,
	}
	SendMapleJuiceNetworkMessage(conn, &msg)
}

func SendMapleTaskRequest(conn net.Conn, numTasks int, exeFile MapleJuiceExeFile, sdfsIntermediateFilenamePrefix string,
	sdfsSrcDirectory string, taskIndex int) {
	msg := MapleJuiceNetworkMessage{
		MsgType:                        MAPLE_TASK_REQUEST,
		NumTasks:                       numTasks,
		ExeFile:                        exeFile,
		SdfsIntermediateFilenamePrefix: sdfsIntermediateFilenamePrefix,
		SdfsSrcDirectory:               sdfsSrcDirectory,
		CurrTaskIdx:                    taskIndex,
	}
	SendMapleJuiceNetworkMessage(conn, &msg)
}

/*
Sent by the worker node to the leader after it has finished executing its maple task.
It sends back the output file with the key value pairs of the maple task.

Since we assume ony 1 job is ever running at a time, we can just send the taskIndex.
When we add support for multiple jobs running simulataneously, we must send the job id along
with the taskIndex

Parameters:

	conn (net.Conn): 				leader connection
	taskIndex (int):				the task index that this worker was assigned to and processed on
	taskOutputFilePpath (string):	file path to the output file containing key value pairs of the maple task output
*/
func SendMapleTaskResponse(conn net.Conn, taskIndex int, taskOutputFilepath string) {
	fmt.Println("Sending Maple Task Response")
	fileSize := utils.GetFileSize(taskOutputFilepath)
	fmt.Println("File size that I WANT to send is: ", fileSize)

	msg := MapleJuiceNetworkMessage{
		MsgType:            MAPLE_TASK_RESPONSE,
		CurrTaskIdx:        taskIndex,
		TaskOutputFileSize: fileSize,
	}

	SendMapleJuiceNetworkMessage(conn, &msg)

	send_file_err := tcp_net.SendFile(taskOutputFilepath, conn, fileSize)
	if send_file_err != nil {
		log.Fatalln("Failed to send file in SendMapleTaskResponse(). error: ", send_file_err)
	}
}

/*
Parameters:

	juice_task_index (int): essentially the 0-indexed task number that this juice task is. It is only sent to the worker so that the
							worker can send it back and so the leader knows which task was the one that was completed
*/
func SendJuiceTaskRequest(conn net.Conn, juiceExe MapleJuiceExeFile, sdfsIntermediateFilenamePrefix string, assignedKeys []string) {
	msg := MapleJuiceNetworkMessage{
		MsgType:                        JUICE_TASK_REQUEST,
		ExeFile:                        juiceExe,
		SdfsIntermediateFilenamePrefix: sdfsIntermediateFilenamePrefix,
		Keys:                           assignedKeys,
	}
	SendMapleJuiceNetworkMessage(conn, &msg)
}

func SendJuiceTaskResponse(conn net.Conn, taskOutputFilepath string, assignedKeys []string) {
	fmt.Println("INSIDE SendJuiceTaskResponse()")
	filesize := utils.GetFileSize(taskOutputFilepath)
	fmt.Println("File size that I WANT to send is: ", filesize)
	fmt.Println("Assigned keys are: ", assignedKeys)

	msg := MapleJuiceNetworkMessage{
		MsgType:            JUICE_TASK_RESPONSE,
		TaskOutputFileSize: filesize,
		Keys:               assignedKeys,
	}
	SendMapleJuiceNetworkMessage(conn, &msg)

	send_file_err := tcp_net.SendFile(taskOutputFilepath, conn, filesize)
	if send_file_err != nil {
		log.Fatalln("Failed to send file in SendJuiceTaskResponse(). error: ", send_file_err)
	}
}

func SendMapleJuiceNetworkMessage(conn net.Conn, msg *MapleJuiceNetworkMessage) {
	serialized_data, err := utils.SerializeData(*msg)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete SendMapleJuiceNetworkMessage")
		return
	}
	err_send2 := tcp_net.SendMessageData(serialized_data, conn)
	if err_send2 != nil {
		log.Fatalln(err_send2)
	}
}

func ReceiveMJNetworkMessage(reader *bufio.Reader) (*MapleJuiceNetworkMessage, error) {
	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		return nil, err1
		//log.Fatalln("Error reading message data for REREPLICATE_REQUEST - ", err1)
	}

	info_struct, err := DeserializeMapleJuiceNetworkMessage(info)
	if err != nil {
		return nil, err
		//log.Fatal("Error in Deserializing Data. Could not complete Receive REREPLICATE_REQUEST")
	}

	return info_struct, nil
}

func DeserializeMapleJuiceNetworkMessage(byteToDeserialize []byte) (*MapleJuiceNetworkMessage, error) {
	deserialized_struct := new(MapleJuiceNetworkMessage)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}
