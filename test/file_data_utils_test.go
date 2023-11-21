package test

import (
	"cs425_mp4/internal/sdfs"
	"cs425_mp4/internal/utils"
	"fmt"
	"testing"
	"time"
)

func TestSerializeDeserialize(t *testing.T) {
	put_info_struct := sdfs.PutInfoRequest{
		"test_file_name",
		506,
		time.Now().UnixNano(),
	}

	serialized_data_bytes, err := utils.SerializeData(put_info_struct)
	if err != nil {
		t.Error("Error in Serializing Data", err)
	}

	put_info_struct_deserialized, err := sdfs.DeserializePutInfoRequest(serialized_data_bytes)
	if err != nil {
		t.Errorf("Error in Deserializing")
	}

	if put_info_struct.Timestamp != put_info_struct_deserialized.Timestamp {
		t.Errorf("Timestamps before & and after serialization & deserialization are not equal")
		t.Errorf("Timestamp before serialization: %d", put_info_struct.Timestamp)
		t.Errorf("Timestamp after serialization & deserialization: %d ", put_info_struct_deserialized.Timestamp)
	}

	if put_info_struct.Filename != put_info_struct_deserialized.Filename {
		t.Errorf("Filename before & and after serialization & deserialization are not equal")
		t.Errorf("Filename before serialization: %s", put_info_struct.Filename)
		t.Errorf("Filename after serialization & deserialization: %s ", put_info_struct_deserialized.Filename)
	}

	if put_info_struct.Filesize != put_info_struct_deserialized.Filesize {
		t.Errorf("Filesize before & and after serialization & deserialization are not equal")
		t.Errorf("Filesize before serialization: %d", put_info_struct.Filesize)
		t.Errorf("Filesize after serialization & deserialization: %d ", put_info_struct_deserialized.Filesize)
	}
}

func TestGetFileSize(t *testing.T) {
	// only working for abs file paths here, but when I actually run it the local file path works
	local_file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP3\\cs425_mp4\\scripts\\5mb_demo.txt"
	file_size := utils.GetFileSize(local_file_path)

	fmt.Println("File size: ", file_size)
}

// TODO: test if shard size is greater than file size
func TestSplitShards(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP3\\cs425_mp4\\test\\test_files\\test_text_file.txt"

	nodeId1 := sdfs.NodeID{
		IpAddress:      "492.158.1.38",
		GossipPort:     "8007",
		SDFSServerPort: "8008",
		Timestamp:      time.Now().UnixNano(),
		Hostname:       "Host1",
	}

	nodeId2 := sdfs.NodeID{
		IpAddress:      "112.158.1.38",
		GossipPort:     "8007",
		SDFSServerPort: "8007",
		Timestamp:      time.Now().UnixNano(),
		Hostname:       "Host2",
	}

	nodeId3 := sdfs.NodeID{
		IpAddress:      "192.158.5.38",
		GossipPort:     "8004",
		SDFSServerPort: "8004",
		Timestamp:      time.Now().UnixNano(),
		Hostname:       "Host3",
	}

	shard_num_to_machines_list := make(map[int][]sdfs.NodeID)
	shard_num_to_machines_list[0] = []sdfs.NodeID{nodeId1, nodeId2}
	shard_num_to_machines_list[1] = []sdfs.NodeID{nodeId1}
	shard_num_to_machines_list[2] = []sdfs.NodeID{nodeId1, nodeId3}
	shard_num_to_machines_list[3] = []sdfs.NodeID{nodeId1, nodeId2, nodeId3}

	shard_num_to_shard_struct := sdfs.SplitShards(file_path, shard_num_to_machines_list, utils.GetFileSize(file_path))

	for key, shard_struct := range shard_num_to_shard_struct {
		fmt.Println("Shard Num: ", key)

		// print every element in shard struct
		fmt.Println("Shard Data: ", string(shard_struct.Data))
		fmt.Println("Shard Shard File Name: ", shard_struct.Metadata.ShardFilename)
		fmt.Println("Shard Index: ", shard_struct.Metadata.ShardIndex)
		fmt.Println("Shard Size: ", shard_struct.Metadata.Size) // should be 50 for every element besides last
		fmt.Println("Shard SDFS File Name: ", shard_struct.Metadata.SdfsFilename)
		fmt.Println()
	}
}

func TestSplitShardsSizeBig(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP3\\cs425_mp4\\test\\test_files\\test_text_file.txt"

	nodeId1 := sdfs.NodeID{
		IpAddress:      "492.158.1.38",
		GossipPort:     "8007",
		SDFSServerPort: "8008",
		Timestamp:      time.Now().UnixNano(),
		Hostname:       "Host1",
	}

	nodeId2 := sdfs.NodeID{
		IpAddress:      "112.158.1.38",
		GossipPort:     "8007",
		SDFSServerPort: "8007",
		Timestamp:      time.Now().UnixNano(),
		Hostname:       "Host2",
	}

	nodeId3 := sdfs.NodeID{
		IpAddress:      "192.158.5.38",
		GossipPort:     "8004",
		SDFSServerPort: "8004",
		Timestamp:      time.Now().UnixNano(),
		Hostname:       "Host3",
	}

	shard_num_to_machines_list := make(map[int][]sdfs.NodeID)
	shard_num_to_machines_list[0] = []sdfs.NodeID{nodeId1, nodeId2, nodeId3}

	shard_num_to_shard_struct := sdfs.SplitShards(file_path, shard_num_to_machines_list, utils.GetFileSize(file_path))

	for key, shard_struct := range shard_num_to_shard_struct {
		fmt.Println("Shard Num: ", key)

		// print every element in shard struct
		fmt.Println("Shard Data: ", string(shard_struct.Data))
		fmt.Println("Shard Shard File Name: ", shard_struct.Metadata.ShardFilename)
		fmt.Println("Shard Index: ", shard_struct.Metadata.ShardIndex)
		fmt.Println("Shard Size: ", shard_struct.Metadata.Size) // should be 50 for every element besides last
		fmt.Println("Shard SDFS File Name: ", shard_struct.Metadata.SdfsFilename)
		fmt.Println()
	}
}

func TestGetShardNumToShardData(t *testing.T) {
	local_file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP3\\cs425_mp4\\test\\test_files\\test_text_file.txt"

	shard_num_to_data := sdfs.GetShardNumToShardData(local_file_path)

	for key, val := range shard_num_to_data {
		fmt.Println("shard num: ", key)
		fmt.Println("Shard Data: ", string(val))
		fmt.Println()
	}
}

func TestGetShardNumToShardDataBigSize(t *testing.T) {
	local_file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP3\\cs425_mp4\\test\\test_files\\test_text_file.txt"

	shard_num_to_data := sdfs.GetShardNumToShardData(local_file_path)

	for key, val := range shard_num_to_data {
		fmt.Println("shard num: ", key)
		fmt.Println("Shard Data: ", string(val))
		fmt.Println()
	}
}
