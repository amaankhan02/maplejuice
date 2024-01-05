package maplejuice

import (
	"log"
	"os"
	"path/filepath"
	"sync"
)

/*
FileSystemService contains information and functions regarding the data
in the *local* node. That is, the current machine.
*/
type FileSystemService struct {
	// path to the root directory that contains the data that this node holds for the SDFS
	RootDirPath string

	// HashMap of Filename to a list of ShardMetaData's belonging to that file, stored on this node
	ShardMetadatas    map[string][]ShardMetaData
	ShardMetaDataLock sync.Mutex
}

func NewFileSystemService(rootDirPath string) *FileSystemService {
	dn := &FileSystemService{}
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
func (fss *FileSystemService) GetShards(sdfs_filename string) []Shard {
	fss.ShardMetaDataLock.Lock()
	metadatas, ok := fss.ShardMetadatas[sdfs_filename]
	fss.ShardMetaDataLock.Unlock()
	if !ok {
		log.Fatalln("GetShards(): Invalid sdfs_filename requested! Does not exist!")
	}
	shards := make([]Shard, 0)

	for _, md := range metadatas {
		// read the file into a Shard and append it to a list
		shards = append(shards, fss.ReadShard(md))
	}
	return shards
}

/*
Delete all Shards associated with the sdfs_filename from the disk and remove
it from the FileSystemService map of shard meta datas
*/
func (fss *FileSystemService) DeleteAllShards(sdfs_filename string) {
	fss.ShardMetaDataLock.Lock()
	defer fss.ShardMetaDataLock.Unlock()
	// delete from disk
	// TODO: should i return an error instead of doing log.fatal
	// delete sdfs_filename from the map
	metadatas, ok := fss.ShardMetadatas[sdfs_filename]
	if !ok {
		log.Fatalln("Invalid maplejuice Filename - does not exist")
	}

	for _, md := range metadatas {
		err := os.Remove(md.ShardFilename)
		if err != nil {
			log.Fatalf("Failed to remove file %s\n", md.ShardFilename)
		}
	}

	delete(fss.ShardMetadatas, sdfs_filename)
}

/*
Given the metadata of a shard, it finds it in the disk and reads it into memory
*/
func (fss *FileSystemService) ReadShard(shardMetadata ShardMetaData) Shard {
	fullPath := filepath.Join(fss.RootDirPath, shardMetadata.ShardFilename)
	data, err := os.ReadFile(fullPath) // TODO:
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
so this function can be used to write the shard to our maplejuice directory locally and keep track of it
as well
*/
func (fss *FileSystemService) WriteShard(shard Shard) {
	// write shard to disk
	writePath := filepath.Join(fss.RootDirPath, shard.Metadata.ShardFilename)
	err := os.WriteFile(writePath, shard.Data, os.ModePerm) // write file with Read and Write permissions
	if err != nil {
		log.Fatalln("WriteShard(): failed to write file to disk...")
	}

	// record the shard in our FileSystemService
	fss.ShardMetaDataLock.Lock()
	fss.ShardMetadatas[shard.Metadata.SdfsFilename] = append(
		fss.ShardMetadatas[shard.Metadata.SdfsFilename], shard.Metadata)
	fss.ShardMetaDataLock.Unlock()
}

/*
Return a list of files (filenames) that are currently being
stored on this machine.
*/
func (fss *FileSystemService) GetAllSDFSFilenames() []string {
	keys := make([]string, 0)
	fss.ShardMetaDataLock.Lock()
	for filename, _ := range fss.ShardMetadatas {
		keys = append(keys, filename)
	}
	fss.ShardMetaDataLock.Unlock()
	return keys
}
