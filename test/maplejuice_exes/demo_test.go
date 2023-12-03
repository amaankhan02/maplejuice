package test

//
//import (
//	"bufio"
//	"cs425_mp4/cmd/juices/juice_demo_phase1"
//	"cs425_mp4/cmd/juices/juice_demo_phase2"
//	"cs425_mp4/cmd/maples/maple_demo_phase1"
//	"cs425_mp4/cmd/maples/maple_demo_phase2"
//	"fmt"
//	"os"
//	"testing"
//)
//
//func TestMapleDemoPhase1(t *testing.T) {
//	num_lines := 27
//	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\Traffic_Signal_Intersections.csv"
//	file, err := os.Open(file_path)
//	if err != nil {
//		fmt.Errorf("Error opening file: %s", err)
//	}
//	defer file.Close()
//
//	// Create a scanner to read the file line by line
//	filescanner := bufio.NewScanner(file)
//	X := "Fiber"
//
//	result := maple_demo_phase1.MapleDemoPhase1(filescanner, num_lines, X)
//	maple_demo_phase1.PrintKeyValuePairs(result)
//}
//
//func TestJuiceDemoPhase1(t *testing.T) {
//	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_demo_phase1_test.txt"
//	file, err := os.Open(file_path)
//	if err != nil {
//		fmt.Errorf("Error opening file: %s", err)
//	}
//	defer file.Close()
//
//	// Create a scanner to read the file line by line
//	filescanner := bufio.NewScanner(file)
//	result := juice_demo_phase1.JuiceDemoTest1(filescanner)
//
//	juice_demo_phase1.PrintKeyValuePairs(result)
//}
//
//func TestMapleDemoPhase2(t *testing.T) {
//	num_lines := 4
//	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\maple_demo_phase2_test.txt"
//	file, err := os.Open(file_path)
//	if err != nil {
//		fmt.Errorf("Error opening file: %s", err)
//	}
//	defer file.Close()
//
//	// Create a scanner to read the file line by line
//	filescanner := bufio.NewScanner(file)
//
//	maple_demo_phase2.MapleDemoPhase2(filescanner, num_lines)
//}
//
//func TestJuiceDemoPhase2(t *testing.T) {
//	file_path := "C:\\Users\\samaa\\Documents\\2023-2024\\DistributedSystems\\MP4\\cs425_mp4\\test\\test_files\\juice_demo_phase2_test.txt"
//	file, err := os.Open(file_path)
//	if err != nil {
//		fmt.Errorf("Error opening file: %s", err)
//	}
//	defer file.Close()
//
//	// Create a scanner to read the file line by line
//	filescanner := bufio.NewScanner(file)
//	juice_demo_phase2.JuicePhase2(filescanner)
//}
