package test

import (
	"bufio"
	maples_exe "cs425_mp4/cmd/maples"
	"fmt"
	"os"
	"testing"
)

func TestMapleSQLJoin(t *testing.T) {
	num_lines := 4
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\maple_sql_join1_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	column := "lastname"

	result := maples_exe.MapleSQLJoin1(filescanner, column, num_lines)
	maples_exe.PrintKeyValPairsSQLJoin(result)
}

func TestMapleSQLJoin3(t *testing.T) {
	num_lines := 6
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\maple_sql_join3_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	column := "lastname"

	result := maples_exe.MapleSQLJoin3(filescanner, column, num_lines)

	maples_exe.PrintKeyValPairsSQLFilter(result)

}
