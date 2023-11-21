package utils

import (
	"bytes"
	//"cs425_mp3/internal/sdfs"
	"encoding/gob"
	"fmt"
	"os"
)

func GetFileSize(local_file_path string) int64 {
	// open file
	file, err := os.Open(local_file_path)
	if err != nil {
		fmt.Println("Error opening the file: ", err)
		return 0.0
	}
	defer file.Close()

	// get file information
	file_info, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file information: ", err)
		return 0.0
	}

	// get file size - in bytes
	file_size := file_info.Size()

	return file_size
}

func SerializeData(structToSerialize interface{}) ([]byte, error) {
	binary_buff := new(bytes.Buffer)

	encoder := gob.NewEncoder(binary_buff)
	err := encoder.Encode(structToSerialize)
	if err != nil {
		return nil, err
	}

	serialized_data := binary_buff.Bytes()
	return serialized_data, nil
}
