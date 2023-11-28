package test

import (
	"bufio"
	juice_exe "cs425_mp4/cmd/juices"
	"cs425_mp4/cmd/maples"
	"fmt"
	"os"
	"testing"
)

func TestMapleExeWordCount(t *testing.T) {
	num_lines := 2
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\scripts\\input_data_25.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	result := maples_exe.MapleWordCount(filescanner, num_lines)
	maples_exe.PrintKeyValuePairs(result)
}

func TestJuiceExeWordCount(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_exe_test_file.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	word_to_word_count := juice_exe.JuiceWordCount(filescanner)

	maples_exe.PrintKeyValuePairs(word_to_word_count)
}
