package test

import (
	"bufio"
	juice_word_count "cs425_mp4/cmd/juices/juice_word_count/"
	maple_word_count "cs425_mp4/cmd/maples/maple_word_count/"
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
	result := maple_word_count.MapleWordCount(filescanner, num_lines)
	maple_word_count .PrintKeyValuePairs(result)
}

func TestJuiceExeWordCount(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_word_count_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	word_to_word_count := juice_word_count.JuiceWordCount(filescanner)

	maple_word_count.PrintKeyValuePairs(word_to_word_count)
}
