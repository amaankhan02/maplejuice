package sdfs

import (
	"bufio"
	"bytes"
	"cs425_mp4/internal/core"
	"cs425_mp4/internal/tcp_net"
	"cs425_mp4/internal/utils"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)

/*
Provides an API that follows the SDFS Network Protocol to send and receive messages of different types

The protocol for messages follow the format:
	[message type][size][data]
Where
	[message type] => 1 byte representing the type of the [data]. Represented by the SDFSNetMessageType type below
	[size] 		   => 4 bytes representing the size of [data] so that the TCP reader knows how many bytes to read
	[data]		   => [size] number of bytes representing the data. This data is different depending on the
					  [message type]. For instance, if message type = GET_REQUEST, then the corresponding
						data would be the serialized struct GetRequest (all defined below)
					  [data] is always a struct that is serialized in an array of bytes so it can be sent over the
					  network. The type of struct depends on [message type]

[data] can be deserialized into the corresponding struct based on the [message type]. This struct should
contain all the information needed to perform the operation or read some value or execute something.
*/

//type SDFSNetMessageType byte

/*
INFO types are for client-leader communication
DATA types are for client-node communication after the leader tells it which nodes to contact
ACK_RESPONSE is used whenever a node wants to send an acknowledgement back for whatever reason
  - when the client sends a PUT_DATA_REQUEST to a node, the node will respond with an ACK_RESPONSE
    when it's done with the operation
  - when the client is down performing the GET/PUT/DELETE operation, it will send an ACK_RESPONSE
    back to the leader to notify that this operation has been completed
*/
const (
	GET_INFO_REQUEST     byte = 0x1
	GET_INFO_RESPONSE    byte = 0x2
	PUT_INFO_REQUEST     byte = 0x3
	PUT_INFO_RESPONSE    byte = 0x4
	DELETE_INFO_REQUEST  byte = 0x5
	DELETE_INFO_RESPONSE byte = 0x6

	GET_DATA_REQUEST    byte = 0x7
	GET_DATA_RESPONSE   byte = 0x8
	PUT_DATA_REQUEST    byte = 0x9
	DELETE_DATA_REQUEST byte = 0xA

	LS_REQUEST            byte = 0xB
	LS_RESPONSE           byte = 0xC
	MULTIREAD_REQUEST     byte = 0xD // sent to start multi-read
	REREPLICATE_REQUEST   byte = 0xE
	REREPLICATE_RESPONSE  byte = 0x11
	PREFIX_MATCH_REQUEST  byte = 0x12
	PREFIX_MATCH_RESPONSE byte = 0x13

	ACK_RESPONSE byte = 0xFF
)

type ReReplicateRequest struct {
	SdfsFilename            string
	NewPossibleReplicaNodes []core.NodeID
	NumNewReplicasRequired  int
}

type ReReplicateResponse struct {
	SdfsFilename  string
	WasSuccessful bool
	NewReplicas   []core.NodeID // list of the new replicas that it successfully saved the sdfs_file to
}

type MultiReadRequest struct {
	SdfsFilename  string
	LocalFilename string
}

// --------------------------- LS STRUCTS ----------------------------------------
type LsRequest struct {
	SdfsFilename string
}

type LsResponse struct {
	SdfsFilename string
	Replicas     []core.NodeID
}

// --------------------------- PREFIX MATCH STRUCTS ----------------------------------------
type PrefixMatchRequest struct {
	SdfsFilenamePrefix string
}

type PrefixMatchResponse struct {
	SdfsFilenames []string // strinsg with the matching prefix
}

// --------------------------- PUT STRUCTS ----------------------------------------
type PutInfoRequest struct {
	SdfsFilename        string
	ClientLocalFilename string // the local filename from the sender
	Filesize            int64
	Timestamp           int64
	ClientID            core.NodeID
}

type PutInfoResponse struct {
	Shard_num_to_machines_list map[int][]core.NodeID
	SdfsFilename               string
	ClientLocalFilename        string // the local filename from the sender
}

type PutDataRequest struct {
	Sharded_data Shard
}

// ------------------------- GET STRUCTS ------------------------------------

type GetInfoRequest struct {
	SdfsFilename  string      // the file in SDFS that we want
	LocalFilename string      // the local filename in the client in which we want the file to be saved
	Timestamp     int64       // timestamp of the request
	ClientID      core.NodeID // ID of the client that made this request - so that we know who to send the response back to
}

type GetInfoResponse struct {
	SdfsFilename        string // the maplejuice filename of the file requested from the maplejuice file system
	ClientLocalFilename string // filename to save to in the client's local machine
	NodeIds             []core.NodeID
}

type GetDataRequest struct {
	Filename string // sdfs_filename
}

type GetDataResponse struct {
	Shards                              []Shard // send back the datablcok
	TotalNumberOfShardsInActualSDFSFile int     // the total number of expected shards in the sfds file needed to reconstruct the file
}

// -------------------------------- DELETE STRUCTS ----------------------------------------

type DeleteInfoRequest struct {
	SdfsFilename string
	Timestamp    int64
	ClientID     core.NodeID
}

type DeleteInfoResponse struct {
	Replicas     []core.NodeID
	SdfsFilename string
}

type DeleteDataRequest struct {
	Filename string // sdfs_filename
}

// ---------------------------------- ACK STRUCT -----------------------------------------

type Ack struct {
	// we can differentiate based on the core.NodeID since we will assume that when a client makes some file operation
	// request, they will not make another one until they receive an ACK that it was successful
	SenderNodeId   core.NodeID // optional
	Message        string      // optional
	WasSuccessful  bool        // to indicate if the operation was successful or not
	AdditionalInfo string      // any additional info to store. File Operation ACKs will store the SDFS-FILENAME here
	StartTime      int64       // time the request was initially made
}

// --------------------------------------------- ReReplicate ROUTING -------------------------------------------------
func SendReReplicateResponse(conn net.Conn, sdfsFilename string, newReplicas []core.NodeID, wasSuccessful bool) {
	info := ReReplicateResponse{
		SdfsFilename:  sdfsFilename,
		NewReplicas:   newReplicas,
		WasSuccessful: wasSuccessful,
	}

	serialized_data, err := utils.SerializeData(info)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send reReplicate response")
		return
	}
	err_send := tcp_net.SendMessageType(REREPLICATE_RESPONSE, conn)
	if err_send != nil {
		log.Fatalln(err_send)
	}
	err_send2 := tcp_net.SendMessageData(serialized_data, conn)
	if err_send2 != nil {
		log.Fatalln(err_send2)
	}
}

func ReceiveReReplicateResponse(reader *bufio.Reader, shouldReadMessageType bool) (*ReReplicateResponse, error) {
	if shouldReadMessageType {
		msgType, err := tcp_net.ReadMessageType(reader)
		if err != nil {
			return nil, err
			//log.Fatalln("(ReceiveReReplicateRequest()): Failed to read message type")
		}
		if msgType != REREPLICATE_RESPONSE {
			return nil, errors.New("received message type != RE-REPLICATE RESPONSE")
			//log.Fatalln("msgType != REREPLICATE_REQUEST!")
		}
	}

	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		return nil, err1
		//log.Fatalln("Error reading message data for REREPLICATE_REQUEST - ", err1)
	}

	info_struct, err := DeserializeReReplicateResponse(info)
	if err != nil {
		return nil, err
		//log.Fatal("Error in Deserializing Data. Could not complete Receive REREPLICATE_REQUEST")
	}

	return info_struct, nil
}

func SendReReplicateRequest(conn net.Conn, sdfsFilename string, newPossibleReplicas []core.NodeID, numNewReplicasRequired int) {
	info := ReReplicateRequest{
		SdfsFilename:            sdfsFilename,
		NewPossibleReplicaNodes: newPossibleReplicas,
		NumNewReplicasRequired:  numNewReplicasRequired,
	}

	serialized_data, err := utils.SerializeData(info)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send ReReplicate Request")
		return
	}
	err_send := tcp_net.SendMessageType(REREPLICATE_REQUEST, conn)
	if err_send != nil {
		log.Fatalln(err_send)
	}
	err_send2 := tcp_net.SendMessageData(serialized_data, conn)
	if err_send2 != nil {
		log.Fatalln(err_send2)
	}
}

func ReceiveReReplicateRequest(reader *bufio.Reader, shouldReadMessageType bool) *ReReplicateRequest {
	if shouldReadMessageType {
		msgType, err := tcp_net.ReadMessageType(reader)
		if err != nil {
			log.Fatalln("(ReceiveReReplicateRequest()): Failed to read message type")
		}
		if msgType != REREPLICATE_REQUEST {
			log.Fatalln("msgType != REREPLICATE_REQUEST!")
		}
	}

	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln("Error reading message data for REREPLICATE_REQUEST - ", err1)
	}

	info_struct, err := DeserializeReReplicateRequest(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive REREPLICATE_REQUEST")
	}

	return info_struct
}

// --------------------------------------------- MultiRead ROUTING -------------------------------------------------
func SendMultiReadRequest(conn net.Conn, sdfsfilename string, localFilename string) {
	info := MultiReadRequest{SdfsFilename: sdfsfilename, LocalFilename: localFilename}

	serialized_data, err := utils.SerializeData(info)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send MultiRead Request")
		return
	}
	err_send := tcp_net.SendMessageType(MULTIREAD_REQUEST, conn)
	if err_send != nil {
		log.Fatalln(err_send)
	}
	err_send2 := tcp_net.SendMessageData(serialized_data, conn)
	if err_send2 != nil {
		log.Fatalln(err_send2)
	}
}

func ReceiveMultiReadRequest(reader *bufio.Reader, shouldReadMessageType bool) *MultiReadRequest {
	if shouldReadMessageType {
		msgType, err := tcp_net.ReadMessageType(reader)
		if err != nil {
			log.Fatalln("(ReceiveMultiReadRequest()): Failed to read message type")
		}
		if msgType != MULTIREAD_REQUEST {
			log.Fatalln("msgType != MULTIREAD_REQUEST!")
		}
	}

	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln("Error reading message data for MULTIREAD_REQUEST - ", err1)
	}

	info_struct, err := DeserializeMultiReadRequest(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive MULTIREAD REQUEST")
	}

	return info_struct
}

// --------------------------------------------- LS ROUTING -------------------------------------------------

func SendLsRequest(conn net.Conn, sdfsfilename string) {
	info := LsRequest{SdfsFilename: sdfsfilename}

	serialized_data, err := utils.SerializeData(info)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send LS Request")
		return
	}
	err_send := tcp_net.SendMessageType(LS_REQUEST, conn)
	if err_send != nil {
		log.Fatalln(err_send)
	}
	err_send2 := tcp_net.SendMessageData(serialized_data, conn)
	if err_send2 != nil {
		log.Fatalln(err_send2)
	}
}

func ReceiveLsRequest(reader *bufio.Reader, shouldReadMessageType bool) *LsRequest {
	if shouldReadMessageType {
		msgType, err := tcp_net.ReadMessageType(reader)
		if err != nil {
			log.Fatalln("(ReceiveLsRequest()): Failed to read message type")
		}
		if msgType != LS_REQUEST {
			log.Fatalln("msgType != LS_REQUEST!")
		}
	}

	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln("Error reading message data for LsRequest - ", err1)
	}

	info_struct, err := DeserializeLsRequest(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive LS REQUEST")
	}

	return info_struct
}

func SendLsResponse(conn net.Conn, sdfsfilename string, replicas []core.NodeID) {
	info := LsResponse{SdfsFilename: sdfsfilename, Replicas: replicas}

	serialized_data, err := utils.SerializeData(info)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send LS Request")
		return
	}
	err_send := tcp_net.SendMessageType(LS_RESPONSE, conn)
	if err_send != nil {
		log.Fatalln(err_send)
	}
	err_send2 := tcp_net.SendMessageData(serialized_data, conn)
	if err_send2 != nil {
		log.Fatalln(err_send2)
	}
}

func ReceiveLsResponse(reader *bufio.Reader, shouldReadMessageType bool) *LsResponse {
	if shouldReadMessageType {
		msgType, err := tcp_net.ReadMessageType(reader)
		if err != nil {
			log.Fatalln("(ReceiveLsResponse()): Failed to read message type")
		}
		if msgType != LS_RESPONSE {
			log.Fatalln("msgType != LS_RESPONSE!")
		}
	}

	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln("Error reading message data for LsResponse - ", err1)
	}

	info_struct, err := DeserializeLsResponse(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Ls Response")
	}

	return info_struct
}

// --------------------------------------------- PREFIX MATCH ROUTING -------------------------------------------------

func SendPrefixMatchRequest(conn net.Conn, prefix string) {
	info := PrefixMatchRequest{SdfsFilenamePrefix: prefix}

	serialized_data, err := utils.SerializeData(info)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send LS Request")
		return
	}
	err_send := tcp_net.SendMessageType(PREFIX_MATCH_REQUEST, conn)
	if err_send != nil {
		log.Fatalln(err_send)
	}
	err_send2 := tcp_net.SendMessageData(serialized_data, conn)
	if err_send2 != nil {
		log.Fatalln(err_send2)
	}
}

func ReceivePrefixMatchRequest(reader *bufio.Reader, shouldReadMessageType bool) *PrefixMatchRequest {
	if shouldReadMessageType {
		msgType, err := tcp_net.ReadMessageType(reader)
		if err != nil {
			log.Fatalln("(ReceivePrefixMatchRequest()): Failed to read message type")
		}
		if msgType != PREFIX_MATCH_REQUEST {
			log.Fatalln("msgType != PREFIX_MATCH_REQUEST!")
		}
	}

	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln("Error reading message data for PREFIX_MATCH_REQUEST - ", err1)
	}

	info_struct, err := DeserializePrefixMatchRequest(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive PREFIX_MATCH_REQUEST")
	}

	return info_struct
}

func SendPrefixMatchResponse(conn net.Conn, filenames []string) {
	info := PrefixMatchResponse{
		SdfsFilenames: filenames,
	}

	serialized_data, err := utils.SerializeData(info)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete")
		return
	}
	err_send := tcp_net.SendMessageType(PREFIX_MATCH_RESPONSE, conn)
	if err_send != nil {
		log.Fatalln(err_send)
	}
	err_send2 := tcp_net.SendMessageData(serialized_data, conn)
	if err_send2 != nil {
		log.Fatalln(err_send2)
	}
}

func ReceivePrefixMatchResponse(reader *bufio.Reader, shouldReadMessageType bool) *PrefixMatchResponse {
	if shouldReadMessageType {
		msgType, err := tcp_net.ReadMessageType(reader)
		if err != nil {
			log.Fatalln("(ReceivePrefixMatchResponse()): Failed to read message type")
		}
		if msgType != PREFIX_MATCH_RESPONSE {
			log.Fatalln("msgType != PREFIX_MATCH_RESPONSE!")
		}
	}

	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln("Error reading message data for PrefixMatchResponse - ", err1)
	}

	info_struct, err := DeserializePrefixMatchResponse(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive PrefixMatchResponse")
	}

	return info_struct
}

// --------------------------------------------- PUT ROUTING -------------------------------------------------

/*
PUT operation:
Called by Client
Send: request to get information about what machines to send file to
Data to Send:
  - Filename
  - filesize
  - Timestamp
*/
func SendPutInfoRequest(conn net.Conn, sdfsfilename string, localfilename string, filesize int64, clientId core.NodeID) {
	// send time over as well as the parameters
	current_time := time.Now().UnixNano()

	put_info := PutInfoRequest{
		SdfsFilename:        sdfsfilename,
		ClientLocalFilename: localfilename,
		Filesize:            filesize,
		Timestamp:           current_time,
		ClientID:            clientId,
	}

	serialized_data, err := utils.SerializeData(put_info)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send Put Info Request")
		return
	}
	err_send := tcp_net.SendMessageType(PUT_INFO_REQUEST, conn)
	if err_send != nil {
		log.Fatalln(err_send)
	}
	err_send2 := tcp_net.SendMessageData(serialized_data, conn)
	if err_send2 != nil {
		log.Fatalln(err_send2)
	}
}

/*
when doing PUT operation:
function called by leader
Recieve: request for information about what machines to send file to
Data Recieved:
  - Filename
  - filesize
  - Timestamp
*/
func ReceivePutInfoRequest(reader *bufio.Reader) *PutInfoRequest {
	//reader := bufio.NewReader(conn)
	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln("Error reading message data for PutInfoRequest - ", err1)
	}

	info_struct, err := DeserializePutInfoRequest(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Put Info Request")
	}

	return info_struct
}

/*
PUT operation:
Called by leader
Send: information about what machines to contact back to client
Data to Send:
  - Map
  - Key = shard number
  - val = list of machines to send shard to
*/
func SendPutInfoResponse(conn net.Conn, shard_num_to_machines_list map[int][]core.NodeID, sdfs_filename string, clientLocalFilename string) {
	put_info_response := PutInfoResponse{
		Shard_num_to_machines_list: shard_num_to_machines_list,
		SdfsFilename:               sdfs_filename,
		ClientLocalFilename:        clientLocalFilename,
	}

	// serialize Filename, filesize, and current item
	serialized_data, err := utils.SerializeData(put_info_response)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send Put Info Request")
		return
	}

	err2 := tcp_net.SendMessageType(PUT_INFO_RESPONSE, conn)
	if err2 != nil {
		log.Fatalln(err2)
	}
	err3 := tcp_net.SendMessageData(serialized_data, conn)
	if err3 != nil {
		log.Fatalln(err3)
	}
}

/*
when doing PUT operation:
Called by client
Receive: what machines to contact
  - Map
  - key = shard number
  - val = list of machines to send shard to
*/
func ReceivePutInfoResponse(reader *bufio.Reader) *PutInfoResponse {
	//reader := bufio.NewReader(conn)
	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln(err1)
	}

	info_struct, err := DeserializePutInfoResponse(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Put Info Response")
	}

	return info_struct
}

/*
when doing PUT operation:
Called by client
Send: shard & request to datanode to store shard
Data to Send:
  - sharded data
*/
func SendPutDataRequest(conn net.Conn, sharded_data *Shard) {
	put_data_request := PutDataRequest{
		*sharded_data,
	}

	// serialize Filename, filesize, and current item
	serialized_data, err := utils.SerializeData(put_data_request)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send Put Data Request")
		return
	}

	err1 := tcp_net.SendMessageType(PUT_DATA_REQUEST, conn)
	if err1 != nil {
		log.Fatalln("Failed to send message type PUT DATA REQUEST. Error: ", err1)
	}
	err2 := tcp_net.SendMessageData(serialized_data, conn)
	if err2 != nil {
		log.Fatalln("Failed to send message data. Error: ", err2)
	}
}

/*
when doing PUT operation:
Called by node
Receieve: Shards to store in the datanode
Data recieved:
  - shard
*/
func ReceivePutDataRequest(reader *bufio.Reader) *PutDataRequest {
	// read data from socket into 'info'
	//reader := bufio.NewReader(conn)
	info, err := tcp_net.ReadMessageData(reader)
	if err != nil {
		log.Fatalln(err)
	}

	// deserialize byte array 'info' into generic struct, and then type cast to PutDataRequest struct
	data_struct, err2 := DeserializePutDataRequest(info)
	if err2 != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Put Data Request")
	}

	return data_struct
}

// ---------------------------------------------- GET ROUTING ------------------------------------------

/*
SendGetInfoRequest
when doing GET operation:
Called by client
Send: shard & request to datanode to store shard
Data to Send:
  - Filename
  - Timestamp
*/
func SendGetInfoRequest(conn net.Conn, sdfsFilename string, localFilename string, clientId core.NodeID) {
	// send time over as well as the parameters
	current_time := time.Now().UnixNano()

	get_info := GetInfoRequest{
		sdfsFilename,
		localFilename,
		current_time,
		clientId,
	}

	serialized_data, err := utils.SerializeData(get_info)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send Get Info Request")
		return
	}

	err1 := tcp_net.SendMessageType(GET_INFO_REQUEST, conn)
	if err1 != nil {
		log.Fatalln(err1)
	}
	err2 := tcp_net.SendMessageData(serialized_data, conn)
	if err2 != nil {
		log.Fatalln(err2)
	}
}

/*
when doing GET operation:
Called by leader
Data Recieved:
  - Filename
  - Timestamp
*/
func ReceiveGetInfoRequest(reader *bufio.Reader) *GetInfoRequest {

	//reader := bufio.NewReader(conn)
	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln(err1)
	}

	info_struct, err := DeserializeGetInfoRequest(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Get Info Request")
	}

	return info_struct
}

/*
when doing GET operation:
Called by Leader
Send: shard & request to datanode to store shard
Data to Send:
  - list of NodeIds
*/
func SendGetInfoResponse(conn net.Conn, machines_to_get_data []core.NodeID, sdfsfilename string, clientLocalfilename string) {
	get_info_response := GetInfoResponse{
		sdfsfilename,
		clientLocalfilename,
		machines_to_get_data,
	}

	serialized_data, err := utils.SerializeData(get_info_response)
	if err != nil {
		log.Fatalln("Error Serializing Data. Could not complete Send Get Info Response")
	}

	err3 := tcp_net.SendMessageType(GET_INFO_RESPONSE, conn)
	if err3 != nil {
		log.Fatalln(err3)
	}
	err2 := tcp_net.SendMessageData(serialized_data, conn)
	if err2 != nil {
		log.Fatalln(err2)
	}
}

/*
when doing GET operation
Called by client
Data recieved:
  - list of NodeIds
*/
func ReceiveGetInfoResponse(reader *bufio.Reader) *GetInfoResponse {
	//reader := bufio.NewReader(conn)
	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln(err1)
	}

	info_struct, err := DeserializeGetInfoResponse(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Get Info Response")
	}

	return info_struct
}

/*
When doing GET operation
Called by Client
Data to send:
  - Filename (so you know what specific Shards to get the info from)
*/
func SendGetDataRequest(conn net.Conn, filename string) {
	get_data_request := GetDataRequest{
		filename,
	}

	// serialize Filename, filesize, and current item
	serialized_data, err := utils.SerializeData(get_data_request)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send Get Data Request")
		return
	}

	tcp_net.SendMessageType(GET_DATA_REQUEST, conn)
	tcp_net.SendMessageData(serialized_data, conn)
}

/*
When doing GET operation
Called by Datanode
Data Recieved:
  - Filename
*/
func ReceiveGetDataRequest(reader *bufio.Reader) *GetDataRequest {
	//reader := bufio.NewReader(conn)
	info, err := tcp_net.ReadMessageData(reader)
	if err != nil {
		log.Fatalln(err)
	}

	info_struct, err2 := DeserializeGetDataRequest(info)
	if err2 != nil {
		log.Fatalln(err)
	}

	return info_struct
}

/*
When doing GET operation
Called by Datanode
Data to Send:
  - Shards
*/
func SendGetDataResponse(conn net.Conn, shards []Shard) {
	get_data_response := GetDataResponse{
		Shards:                              shards,
		TotalNumberOfShardsInActualSDFSFile: 1, // TODO: change this when using more shards!! for now just 1
	}

	serialized_data, err1 := utils.SerializeData(get_data_response)
	if err1 != nil {
		log.Fatalln("Error Serializing Data. Could not complete Send Get Data Response")
	}

	err2 := tcp_net.SendMessageType(GET_DATA_RESPONSE, conn)
	if err2 != nil {
		log.Fatal("Failed to send message type GET_DATA_RESPONSE")
	}
	err := tcp_net.SendMessageData(serialized_data, conn)
	if err != nil {
		log.Fatalln("Failed to send message data")
	}
}

/*
When doing GET operation
Called by Client
Data Recieved:
  - Shards
*/
func ReceiveGetDataResponse(reader *bufio.Reader) *GetDataResponse {
	//reader := bufio.NewReader(conn)
	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln(err1)
	}

	info_struct, err := DeserializeGetDataResponse(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Get Data Response")
	}

	return info_struct
}

// -------------------------------------------------DELETE -------------------------------------------

/*
When doing DELETE operation
Called by client
Data to Send:
  - Filename
  - Timestamp
*/
func SendDeleteInfoRequest(conn net.Conn, sdfsfilename string, clientId core.NodeID) {
	// send time over as well as the parameters
	current_time := time.Now().UnixNano()

	get_data_response := DeleteInfoRequest{
		SdfsFilename: sdfsfilename,
		Timestamp:    current_time,
		ClientID:     clientId,
	}

	serialized_data, err := utils.SerializeData(get_data_response)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Delete Info Request")
		return
	}

	err1 := tcp_net.SendMessageType(DELETE_INFO_REQUEST, conn)
	if err1 != nil {
		log.Fatalln(err1)
	}
	err2 := tcp_net.SendMessageData(serialized_data, conn)
	if err2 != nil {
		log.Fatalln(err2)
	}
}

/*
When doing DELETE operation
Called by leader
Data Recieved:
  - Filename
  - Timestamp
*/
func ReceiveDeleteInfoRequest(reader *bufio.Reader) *DeleteInfoRequest {
	//reader := bufio.NewReader(conn)
	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln(err1)
	}

	info_struct, err := DeserializeDeleteInfoRequest(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Delete Info Request")
	}

	return info_struct
}

/*
When doing DELETE operation
Called by leader
Data to Send:
  - nodeids to find Shards and delete at
*/
func SendDeleteInfoResponse(conn net.Conn, shard_num_to_machines_list []core.NodeID, sdfs_filename string) {
	delete_info_response := DeleteInfoResponse{
		shard_num_to_machines_list, sdfs_filename,
	}

	serialized_data, err := utils.SerializeData(delete_info_response)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send Delete Info Response")
		return
	}

	tcp_net.SendMessageType(DELETE_INFO_RESPONSE, conn)
	tcp_net.SendMessageData(serialized_data, conn)
}

/*
When doing DELETE operation
Called by client
Data Recieved:
  - NodeIds
*/
func ReceiveDeleteInfoResponse(reader *bufio.Reader) *DeleteInfoResponse {
	//reader := bufio.NewReader(conn)
	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln(err1)
	}

	info_struct, err := DeserializeDeleteInfoResponse(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Delete Info Response")
	}

	return info_struct
}

/*
When doing DELETE operations
Called by client
Data to send:
  - Filename
*/
func SendDeleteDataRequest(conn net.Conn, filename string) {
	delete_info_response := DeleteDataRequest{
		filename,
	}

	serialized_data, err := utils.SerializeData(delete_info_response)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send Delete Data Request")
		return
	}

	tcp_net.SendMessageType(DELETE_DATA_REQUEST, conn)
	tcp_net.SendMessageData(serialized_data, conn)
}

func ReceiveDeleteDataRequest(reader *bufio.Reader) *DeleteDataRequest {
	//reader := bufio.NewReader(conn)
	info, err1 := tcp_net.ReadMessageData(reader)
	if err1 != nil {
		log.Fatalln(err1)
	}

	info_struct, err := DeserializeDeleteDataRequest(info)
	if err != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Delete Data Request")
	}

	return info_struct
}

// ------------------------------------------------- ACK  ----------------------------------------
/*
when doing ANY operation where you need to send ACK:
Called by a datanode or client
Send: acknowledgment saying you are done
*/
func SendAckResponse(conn net.Conn, senderId core.NodeID, isSuccess bool, message string, additionalInfo string, startTime int64) {
	ack := Ack{
		Message:        message,
		SenderNodeId:   senderId,
		WasSuccessful:  isSuccess,
		AdditionalInfo: additionalInfo,
		StartTime:      startTime,
	}

	serialized_data, err := utils.SerializeData(ack)
	if err != nil {
		fmt.Println("Error Serializing Data. Could not complete Send Ack Response")
		return
	}

	err2 := tcp_net.SendMessageType(ACK_RESPONSE, conn)
	if err2 != nil {
		log.Fatalln("Failed to send message type ACK")
	}
	err3 := tcp_net.SendMessageData(serialized_data, conn)
	if err3 != nil {
		log.Fatalln("Failed to send message data for ACK")
	}
}

/*
Read an ACK_RESPONSE type and data.

This will read the message type + the message data! The other functions don't do this and assume the type
was already read!
*/
func ReceiveFullAckResponse(reader *bufio.Reader) *Ack {
	//reader := bufio.NewReader(conn)

	msgType, err1 := tcp_net.ReadMessageType(reader)
	if err1 != nil {
		log.Fatalln("Failed to read the message type - should've been ACK_RESPONSE")
	}

	if msgType != ACK_RESPONSE {
		log.Fatalf("Read message type but its NOT EQUAL TO ACK_RESPONSE!. Instead its: %02x", msgType)
	}
	return ReceiveOnlyAckResponseData(reader)
}

/*
Assumes the message type was already read and was an ACK_RESPONSE.
This will therefore only read the ACK struct
*/
func ReceiveOnlyAckResponseData(reader *bufio.Reader) *Ack {
	//reader := bufio.NewReader(conn)
	info, err3 := tcp_net.ReadMessageData(reader)
	if err3 != nil {
		log.Fatalln(err3)
	}

	ack_struct, err4 := DeserializeAck(info)
	if err4 != nil {
		log.Fatal("Error in Deserializing Data. Could not complete Receive Ack Response")
	}

	return ack_struct
}

// ---------------------------------------- DESERIALIZATION ------------------------
func DeserializeReReplicateResponse(byteToDeserialize []byte) (*ReReplicateResponse, error) {
	deserialized_struct := new(ReReplicateResponse)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeReReplicateRequest(byteToDeserialize []byte) (*ReReplicateRequest, error) {
	deserialized_struct := new(ReReplicateRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeMultiReadRequest(byteToDeserialize []byte) (*MultiReadRequest, error) {
	deserialized_struct := new(MultiReadRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeLsRequest(byteToDeserialize []byte) (*LsRequest, error) {
	deserialized_struct := new(LsRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeLsResponse(byteToDeserialize []byte) (*LsResponse, error) {
	deserialized_struct := new(LsResponse)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializePrefixMatchRequest(byteToDeserialize []byte) (*PrefixMatchRequest, error) {
	deserialized_struct := new(PrefixMatchRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializePrefixMatchResponse(byteToDeserialize []byte) (*PrefixMatchResponse, error) {
	deserialized_struct := new(PrefixMatchResponse)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializePutInfoRequest(byteToDeserialize []byte) (*PutInfoRequest, error) {
	deserialized_struct := new(PutInfoRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializePutInfoResponse(byteToDeserialize []byte) (*PutInfoResponse, error) {
	deserialized_struct := new(PutInfoResponse)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializePutDataRequest(byteToDeserialize []byte) (*PutDataRequest, error) {
	deserialized_struct := new(PutDataRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeGetInfoRequest(byteToDeserialize []byte) (*GetInfoRequest, error) {
	deserialized_struct := new(GetInfoRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeGetInfoResponse(byteToDeserialize []byte) (*GetInfoResponse, error) {
	deserialized_struct := new(GetInfoResponse)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeGetDataRequest(byteToDeserialize []byte) (*GetDataRequest, error) {
	deserialized_struct := new(GetDataRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeGetDataResponse(byteToDeserialize []byte) (*GetDataResponse, error) {
	deserialized_struct := new(GetDataResponse)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeDeleteInfoRequest(byteToDeserialize []byte) (*DeleteInfoRequest, error) {
	deserialized_struct := new(DeleteInfoRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeDeleteInfoResponse(byteToDeserialize []byte) (*DeleteInfoResponse, error) {
	deserialized_struct := new(DeleteInfoResponse)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeDeleteDataRequest(byteToDeserialize []byte) (*DeleteDataRequest, error) {
	deserialized_struct := new(DeleteDataRequest)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}

func DeserializeAck(byteToDeserialize []byte) (*Ack, error) {
	deserialized_struct := new(Ack)
	byteBuffer := bytes.NewBuffer(byteToDeserialize)
	decoder := gob.NewDecoder(byteBuffer)

	err := decoder.Decode(deserialized_struct)
	if err != nil {
		return nil, err
	}

	return deserialized_struct, nil
}
