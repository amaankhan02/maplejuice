package test

import (
	"cs425_mp4/internal/maplejuice"
	"cs425_mp4/internal/utils"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func TestExecuteMapleTask(t *testing.T) {

}

func TestCreateTempDirsAndFilesForMapleTask(t *testing.T) {
	mjn := maplejuice.MapleJuiceNode{}
	mapleTaskDirPath, _, _ := mjn.CreateTempDirsAndFilesForMapleTask(1, "some_prefix_", 1)
	fmt.Println("mapleTaskDirPath: ", mapleTaskDirPath)
}

func TestCreateTempDirsAndFilesForJuiceTask(t *testing.T) {
	mjn := maplejuice.MapleJuiceNode{}
	juiceTaskDirPath, file := mjn.CreateTempDirsAndFilesForJuiceTask(1)
	file.Close()
	fmt.Println("juiceTaskDirPath: ", juiceTaskDirPath)
}

func TestDeleteTempDirsAndFilesForJuiceTask(t *testing.T) {
	err := utils.DeleteDirAndAllContents("juicetask-1")
	if err != nil {
		t.Errorf("Error deleting dir: %s", err)
	}
}

func TestDeleteTempDirsAndFilesForMapleTask(t *testing.T) {
	err := utils.DeleteDirAndAllContents("mapletask-1-1-some_prefix_")
	if err != nil {
		t.Errorf("Error deleting dir: %s", err)
	}
}
func TestExecuteMapleExe(t *testing.T) {
	//mjn := maplejuice.MapleJuiceNode{}
	//mapleExeFilepath, filepath_err := filepath.Abs("..\\..\\maple_word_count.exe")
	//if filepath_err != nil {
	//	t.Errorf("Error getting absolute path for maple_exe: %s", filepath_err)
	//}
	//fmt.Println("Full path: ", mapleExeFilepath)
	//var args []string = []string{"", ""}
	//inputFilepath := "..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data.txt"
	//inputFile, err1 := os.OpenFile(inputFilepath, os.O_RDONLY, 0666)
	//if err1 != nil {
	//	t.Errorf("Error opening file: %s", inputFilepath)
	//}
	//
	//outputFilepath := "..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data_OUTPUT.txt"
	//outputFile, err2 := os.OpenFile(outputFilepath, os.O_CREATE|os.O_WRONLY, 0666)
	//if err2 != nil {
	//	t.Errorf("Error opening file: %s", outputFilepath)
	//}

	//mjn.ExecuteMapleExe(mapleExeFilepath, args, inputFile, outputFile, 5)
}

func TestNewExecuteMapleExePrintStdout(t *testing.T) {
	//mjn := maplejuice.MapleJuiceNode{}
	//mapleExeFilepath, filepath_err := filepath.Abs("..\\..\\maple_word_count.exe")
	//if filepath_err != nil {
	//	t.Errorf("Error getting absolute path for maple_exe: %s", filepath_err)
	//}
	//
	//inputFilepath, _ := filepath.Abs("..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data.txt")
	//args := []string{inputFilepath, "2", "5"}
	//mjn.NewExecuteMapleExe(mapleExeFilepath, args, os.Stdout)
}

func TestNewExecuteMapleExePrintToFile(t *testing.T) {
	//mjn := maplejuice.MapleJuiceNode{}
	//mapleExeFilepath, filepath_err := filepath.Abs("..\\..\\maple_word_count.exe")
	//if filepath_err != nil {
	//	t.Errorf("Error getting absolute path for maple_exe: %s", filepath_err)
	//}
	//
	//inputFilepath, _ := filepath.Abs("..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data.txt")
	//args := []string{inputFilepath, "8", "5"}
	//outputFilepath, _ := filepath.Abs("..\\test_files\\maplejuice_node_tests\\maple_exe_word_count_test_data_OUTPUT.txt")
	//outputFile, open_err := os.OpenFile(outputFilepath, os.O_WRONLY|os.O_APPEND, 0666)
	//if open_err != nil {
	//	t.Errorf("Error opening file: %s", outputFilepath)
	//}
	//defer outputFile.Close()
	//mjn.NewExecuteMapleExe(mapleExeFilepath, args, outputFile)
}

func TestNewExecuteJuiceExeOnKey(t *testing.T) {
	mjn := maplejuice.MapleJuiceNode{}
	exeFilePath, _ := filepath.Abs("..\\..\\juice_word_count.exe")
	juiceExe := maplejuice.MapleJuiceExeFile{
		ExeFilePath: exeFilePath,
	}
	inputFilePath := "C:\\Users\\amaan\\Dev\\cs425\\cs425_mp4\\test\\test_files\\maplejuice_node_tests\\juice_exe_word_count_input_test.txt"
	fmt.Println("inputFilePath: ", inputFilePath)
	juiceOutChan := make(chan string, 1)
	mjn.ExecuteJuiceExeOnKey(juiceExe, inputFilePath, juiceOutChan)

	outKV := <-juiceOutChan
	close(juiceOutChan)
	fmt.Println("outKV: ", outKV)
}

func TestExecuteJuiceExeInParallel(t *testing.T) {
	mjn := maplejuice.MapleJuiceNode{}
	exeFilePath, _ := filepath.Abs("..\\..\\juice_word_count.exe")
	juiceExe := maplejuice.MapleJuiceExeFile{
		ExeFilePath: exeFilePath,
	}
	inputFilePath := "C:\\Users\\amaan\\Dev\\cs425\\cs425_mp4\\test\\test_files\\maplejuice_node_tests\\juice_exe_word_count_input_test.txt"
	fmt.Println("inputFilePath: ", inputFilePath)

	var wg sync.WaitGroup
	juiceExeOutputsChan := make(chan string, 3) // buffered channel so that we don't block on the go routines

	// start a goroutine to execute each juice exe
	for i := 0; i < 3; i++ {
		go func(idx int) {
			wg.Add(1)
			mjn.ExecuteJuiceExeOnKey(juiceExe, inputFilePath, juiceExeOutputsChan)
			wg.Done()
		}(i)
		// each task will generate just one key-value pair, which will be returned on the channel
	}

	// close the channel once all goroutines have finished - do it in separate goroutine so that we don't block
	go func() {
		wg.Wait()
		// we must close otherwise the for-loop below where we read from the channel will block forever cuz it will read as long as the channel is open
		close(juiceExeOutputsChan)
	}()

	for result := range juiceExeOutputsChan {
		fmt.Print(result)
	}
}
