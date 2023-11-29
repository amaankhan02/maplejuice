package utils

import (
	"bufio"
	"bytes"
	"log"

	//"cs425_mp4/internal/maplejuice"
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

/*
Counts the number of lines in the file by reading the entire file
line by line. Additionaly, moves the file pointer back to the start before exiting out
of the function.

Parameters:

	file: file to read. Must be opened in read mode. Can NOT be in APPEND mode!
*/
func CountNumLinesInFile(file *os.File) int64 {
	scanner := bufio.NewScanner(file)
	var count int64 = 0

	for scanner.Scan() {
		count += 1
	}

	if err := scanner.Err(); err != nil {
		log.Fatalln("Error reading file:", err)
	}

	_, err := file.Seek(0, 0)
	if err != nil {
		log.Fatalln("Failed to reset file pointer back to 0! - Error: ", err)
	}

	return count
}
