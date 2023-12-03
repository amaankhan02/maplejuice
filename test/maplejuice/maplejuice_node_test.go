package maplejuice

import (
	"cs425_mp4/internal/maplejuice"
	"os"
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
	mapleExeFilepath := "..\\..\\maple_word_count.exe"

	var args []string = []string{"", ""}
	inputFilepath := "..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data.txt"
	inputFile, err1 := os.OpenFile(inputFilepath, os.O_RDONLY, 0666)
	if err1 != nil {
		t.Errorf("Error opening file: %s", inputFilepath)
	}

	outputFilepath := "..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data_OUTPUT.txt"
	outputFile, err2 := os.OpenFile(outputFilepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err2 != nil {
		t.Errorf("Error opening file: %s", outputFilepath)
	}

	mjn.ExecuteMapleExe(mapleExeFilepath, args, inputFile, outputFile, 20)
}
