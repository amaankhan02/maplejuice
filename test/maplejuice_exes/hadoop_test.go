package test

import (
	"bufio"
	"cs425_mp4/cmd/mapper1"
	"cs425_mp4/cmd/mapper2"
	"cs425_mp4/cmd/reducer1"
	"cs425_mp4/cmd/reducer2"
	"fmt"
	"os"
	"testing"
)

func TestMapper1(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\Traffic_Signal_Intersections.csv"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)
	X := "Fiber"

	mapper1.Mapper1(filescanner, X)
}

func TestReducer1(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\mapper1_output.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)

	reducer1.Reducer1(filescanner)
}

func TestMapper2(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\reducer1_output.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)

	mapper2.Mapper2(filescanner)
}

func TestReducer2(t *testing.T) {
	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\mapper2_output.txt"
	file, err := os.Open(file_path)
	if err != nil {
		fmt.Errorf("Error opening file: %s", err)
	}
	defer file.Close()

	// Create a scanner to read the file line by line
	filescanner := bufio.NewScanner(file)

	reducer2.Reducer2(filescanner)

	// FINAL OUTPUT:
	// ,1.6129031
	//None,12.903225
	//Radar,1.6129031
	//Loop/None,3.2258062
	//,1.6129031
	//Loop/Video,6.4516125
	//Loop,38.709675
	//Video,33.870968
}
