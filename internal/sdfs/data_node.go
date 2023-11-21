package sdfs

import (
	"log"
	"os"
	"path/filepath"
)

/*
DataNode contains information and functions regarding the data
in the *local* node. That is, the current machine.
*/
type DataNode struct {
	// path to the root directory that contains the data that this node holds for the SDFS
	RootDirPath string

	// HashMap of Filename to a list of ShardMetaData's belonging to that file, stored on this node
	ShardMetadatas map[string][]ShardMetaData
}

func NewDataNode(rootDirPath string) *DataNode {
	dn := &DataNode{}
	dn.RootDirPath = rootDirPath
	dn.ShardMetadatas = make(map[string][]ShardMetaData)
	return dn
}

/*
This will get all the shard metadatas corresponding to the sdfs_filename, and
then read the Shards from disk into memory into a list of Shard objects. And then
return that.

TODO: this wouldn't work for large files! because we don't want to read everything into memory...
TODO: but for now this is fine. After it works with smaller files, we want to change this
TODO: so that it returns a list of the metadatas instead and the caller should just read into a Shard,
TODO: immediately send it in the stream, and then delete the object, and then repeat. And the message size
TODO: in the TCP message should be calcualted before hand. so there's a bit of work to change for that - DO THIS LATER!
*/
func (this *DataNode) GetShards(sdfs_filename string) []Shard {
	metadatas, ok := this.ShardMetadatas[sdfs_filename]
	if !ok {
		log.Fatalln("GetShards(): Invalid sdfs_filename requested! Does not exist!")
	}
	shards := make([]Shard, 0)

	for _, md := range metadatas {
		// read the file into a Shard and append it to a list
		shards = append(shards, this.ReadShard(md))
	}
	return shards
}

/*
Delete all Shards associated with the sdfs_filename from the disk and remove
it from the DataNode map of shard meta datas
*/
func (this *DataNode) DeleteAllShards(sdfs_filename string) {
	metadatas, ok := this.ShardMetadatas[sdfs_filename]
	if !ok {
		log.Fatalln("Invalid sdfs Filename - does not exist")
	}

	// delete from disk
	for _, md := range metadatas {
		err := os.Remove(md.ShardFilename)
		if err != nil {
			log.Fatalf("Failed to remove file %s\n", md.ShardFilename)
			// TODO: should i return an error instead of doing log.fatal
		}
	}

	// delete sdfs_filename from the map
	delete(this.ShardMetadatas, sdfs_filename)
}

/*
Given the metadata of a shard, it finds it in the disk and reads it into memory
*/
func (this *DataNode) ReadShard(shardMetadata ShardMetaData) Shard {
	fullPath := filepath.Join(this.RootDirPath, shardMetadata.ShardFilename)
	data, err := os.ReadFile(fullPath)
	if err != nil {
		log.Fatalln("Error reading file!")
		// TODO: Change this log.Fatal() to something else.. maybe just return an error
	}
	ret := Shard{
		Data:     data,
		Metadata: shardMetadata,
	}
	return ret
}

/*
In a PUT_DATA_REQUEST, the receiver will receive a Shard object, which includes the ShardMetaData,
so this function can be used to write the shard to our sdfs directory locally and keep track of it
as well
*/
func (this *DataNode) WriteShard(shard Shard) {
	// write shard to disk
	writePath := filepath.Join(this.RootDirPath, shard.Metadata.ShardFilename)
	err := os.WriteFile(writePath, shard.Data, os.ModePerm) // write file with Read and Write permissions
	if err != nil {
		log.Fatalln("WriteShard(): failed to write file to disk...")
	}

	// record the shard in our DataNode
	this.ShardMetadatas[shard.Metadata.SdfsFilename] = append(
		this.ShardMetadatas[shard.Metadata.SdfsFilename], shard.Metadata)
}

/*
Return a list of files (filenames) that are currently being
stored on this machine.
*/
func (this *DataNode) GetAllSDFSFilenames() []string {
	keys := make([]string, 0)

	for filename, _ := range this.ShardMetadatas {
		keys = append(keys, filename)
	}
	return keys
}
