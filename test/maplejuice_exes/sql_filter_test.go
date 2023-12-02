package test

import (
	"bufio"
	juice_exe "cs425_mp4/cmd/juices"
	maples_exe "cs425_mp4/cmd/maples"
	"fmt"
	"os"
	"testing"
)

func TestMapleSQLFilter(t *testing.T) {
	num_lines := 7
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\maple_sql_filter_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	regex := "^K" // starting with letter K
	column := "lastname"

	result := maples_exe.MapleSQLFilter(filescanner, regex, column, num_lines)
	maples_exe.PrintKeyValPairsSQLFilter(result)
}

func TestJuiceSQLFilter(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_sql_filter_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	matched_lines := juice_exe.JuiceSQL(filescanner)

	juice_exe.PrintSliceString(matched_lines)
}
