package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

/*
Miscelanneous Utility functions
*/

// Reads from stdin and trims any additional whitespace on sides and returns as a string
func ReadUserInput() (string, error) {
	reader := bufio.NewReader(os.Stdin)
	userInput, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	userInput = strings.TrimSpace(userInput)
	return userInput, nil
}

// Get the current time as a formatted string as hh:mm:ss:ms
func GetCurrentTime() string {
	// Get the current time
	currentTime := time.Now()

	// Extract hours, minutes, seconds, and milliseconds
	hours := currentTime.Hour()
	minutes := currentTime.Minute()
	seconds := currentTime.Second()
	milliseconds := currentTime.Nanosecond() / 1e6 // Divide nanoseconds by 1e6 to get milliseconds

	// Print the formatted time
	return fmt.Sprintf("%02d:%02d:%02d.%03d", hours, minutes, seconds, milliseconds)
}

func RemoveIthElementFromSlice[T int](s []T, i int) []T {
	// replace ith element with the last element and then return everything up to but not including
	// the last element
	s[i] = s[len(s)-1]
	return s[:len(s)-1]
}

//func CreateDataBlocks(local_file_path string, shards_list []string) []DataBlock {
//	data_blocks_list := make([]DataBlock, 0)
//
//	for i, shard := range shards_list {
//		datablock := DataBlock{
//			shard,
//			i,
//			true,
//			local_file_path,
//			len(shard), // a string is a sequence of bytes so just need to get len(string)
//		}
//
//		fmt.Println("DataBlock: ")
//		fmt.Println(datablock)
//		fmt.Println()
//		data_blocks_list = append(data_blocks_list, datablock)
//	}
//
//	return data_blocks_list
//}

func ShardData(local_file_path string, num_shards int64) []string {
	total_file_size := GetFileSize(local_file_path)

	// For right now split into 4 shards
	// even though we have the num shards declared, there might be one extra one for left overs
	shard_size := total_file_size / num_shards

	// open file
	file, err := os.Open(local_file_path)
	if err != nil {
		fmt.Println("Error in opening file: ", err)
		return nil
	}
	defer file.Close()

	buffer := make([]byte, shard_size)
	shards_list := make([]string, 0)

	for {

		fmt.Println("In Loop")
		n, err := file.Read(buffer)
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Println("Error in reading file: ", err)
			return nil
		}

		// convert the data to a string and append it to the list of shards
		shard := string(buffer[:n])
		shards_list = append(shards_list, shard)
	}

	return shards_list
}
