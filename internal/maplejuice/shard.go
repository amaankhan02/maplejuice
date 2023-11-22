package maplejuice

/*
Stores the metadata regarding the shard associated with 'SdfsFilename'.
*/
type ShardMetaData struct {
	ShardIndex    int    // index of the shard number in this file
	SdfsFilename  string // the maplejuice Filename that this shard belongs to
	ShardFilename string // Filename of this specific shard
	Size          int64
}

/*
Immutable structure. Holds the actual shard file content for an associated
ShardMetaData
*/
type Shard struct {
	Data     []byte
	Metadata ShardMetaData
}
