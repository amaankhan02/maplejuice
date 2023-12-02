package test

import (
	"bufio"
	juice_exe "cs425_mp4/cmd/juices"
	"cs425_mp4/cmd/maples/maple_demo_phase1"
	"cs425_mp4/cmd/maples/maple_demo_phase2"
	maples_exe_word_count "cs425_mp4/cmd/maples/maple_word_count"
	"fmt"
	"os"
	"testing"
)

func TestMapleDemoPhase1(t *testing.T) {
	num_lines := 10
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\maple_demo_phase1_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	X := "Fiber"

	result := maple_demo_phase1.MapleDemoPhase1(filescanner, num_lines, X)
	maple_demo_phase1.PrintKeyValuePairs(result)
}

func TestJuiceDemoPhase1(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_demo_phase1_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	result := juice_exe.JuiceDemoTest1(filescanner)

	maples_exe_word_count.PrintKeyValuePairs(result)
}

func TestMapleDemoPhase2(t *testing.T) {
	num_lines := 3
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\maple_demo_phase2_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)

	result := maple_demo_phase2.MapleDemoPhase2(filescanner, num_lines)
	maple_demo_phase2.PrintKeyValuePairs(result)
}

func TestJuiceDemoPhase2(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_demo_phase2_test.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	result := juice_exe.JuicePhase2(filescanner)

	maple_demo_phase2.PrintKeyValuePairs(result)
}
