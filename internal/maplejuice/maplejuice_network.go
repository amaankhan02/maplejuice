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
	NumTasks                       int    // number of maples or juice tasks (depending on the type)
	ExeFile                        string // maple_exe or juice_exe
	SdfsIntermediateFilenamePrefix string // prefix of the intermediate filenames (output of Maple, input of Juice)
	SdfsSrcDirectory               string // location of input files for Maple
	SdfsDestFilename               string // filename location for output of Juice step where all key-value pairs are appended
	ShouldDeleteJuiceInput         bool   // if true, after the juice phase is done, the sdfs intermediate files will be deleted (inputs for the juice phase)
}

func SendMJNetworkMessage(conn net.Conn, msg *MapleJuiceNetworkMessage) {
	serialized_data, err := utils.SerializeData(*msg)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete SendMJNetworkMessage")
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
