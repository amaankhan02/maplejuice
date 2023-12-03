package test

import (
	"cs425_mp4/internal/maplejuice"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestExecuteMapleTask(t *testing.T) {

}

func TestCreateTempDirsAndFilesForMapleTask(t *testing.T) {

}

func TestCalculateStartAndEndLinesForTask(t *testing.T) {

}

func TestExecuteMapleExe(t *testing.T) {
	mjn := maplejuice.MapleJuiceNode{}
	mapleExeFilepath, filepath_err := filepath.Abs("..\\..\\maple_word_count.exe")
	if filepath_err != nil {
		t.Errorf("Error getting absolute path for maple_exe: %s", filepath_err)
	}
	fmt.Println("Full path: ", mapleExeFilepath)
	var args []string = []string{"", ""}
	inputFilepath := "..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data.txt"
	inputFile, err1 := os.OpenFile(inputFilepath, os.O_RDONLY, 0666)
	if err1 != nil {
		t.Errorf("Error opening file: %s", inputFilepath)
	}

	outputFilepath := "..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data_OUTPUT.txt"
	outputFile, err2 := os.OpenFile(outputFilepath, os.O_CREATE|os.O_WRONLY, 0666)
	if err2 != nil {
		t.Errorf("Error opening file: %s", outputFilepath)
	}

	//mjn.ExecuteMapleExe(mapleExeFilepath, args, inputFile, outputFile, 5)
}

func TestNewExecuteMapleExePrintStdout(t *testing.T) {
	mjn := maplejuice.MapleJuiceNode{}
	mapleExeFilepath, filepath_err := filepath.Abs("..\\..\\maple_word_count.exe")
	if filepath_err != nil {
		t.Errorf("Error getting absolute path for maple_exe: %s", filepath_err)
	}

	inputFilepath, _ := filepath.Abs("..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data.txt")
	args := []string{inputFilepath, "2", "5"}
	//mjn.NewExecuteMapleExe(mapleExeFilepath, args, os.Stdout)
}

func TestNewExecuteMapleExePrintToFile(t *testing.T) {
	mjn := maplejuice.MapleJuiceNode{}
	mapleExeFilepath, filepath_err := filepath.Abs("..\\..\\maple_word_count.exe")
	if filepath_err != nil {
		t.Errorf("Error getting absolute path for maple_exe: %s", filepath_err)
	}

	inputFilepath, _ := filepath.Abs("..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data.txt")
	args := []string{inputFilepath, "8", "5"}
	outputFilepath, _ := filepath.Abs("..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data_OUTPUT.txt")
	outputFile, open_err := os.OpenFile(outputFilepath, os.O_WRONLY|os.O_APPEND, 0666)
	if open_err != nil {
		t.Errorf("Error opening file: %s", outputFilepath)
	}
	defer outputFile.Close()
	//mjn.NewExecuteMapleExe(mapleExeFilepath, args, outputFile)
}
