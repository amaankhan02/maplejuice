package tcp_net

import (
	"cs425_mp4/internal/tcp_net"
	"cs425_mp4/internal/utils"
	"net"
	"sync"
	"testing"
	"time"
)

func sendFileHelper(t *testing.T, sendFilePath string) {
	filesize := utils.GetFileSize(sendFilePath)

	// dial to loopback server on port 8080
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		t.Errorf("Error dialing: %s", err)
	}
	defer conn.Close()

	err = tcp_net.SendFile(sendFilePath, conn, filesize)
	if err != nil {
		t.Errorf("Error sending file: %s", err)
	}
	t.Log("File sent successfully")
}

func readFileHelper(t *testing.T, saveFilepath string, fileBeingSent string) {
	listener, err := net.Listen("tcp", ":"+"8080")
	if err != nil {
		t.Errorf("Error listening: %s", err)
	}

	conn, err := listener.Accept()
	if err != nil {
		t.Errorf("Error accepting: %s", err)
	}
	defer conn.Close()

	expectedFileSize := utils.GetFileSize(fileBeingSent)

	err = tcp_net.ReadFile(saveFilepath, conn, expectedFileSize)
	if err != nil {
		t.Errorf("Error reading file: %s", err)
	}
	t.Log("File read and saved")

	// compare the contents of the 2 files. Read them in a buffer at a time and compare
	// the contents of the buffer
	isFilesEqual, fileerr := utils.AreFilesIdentical(fileBeingSent, saveFilepath)
	if fileerr != nil {
		t.Errorf("Error comparing files: %s", fileerr)
	}
	if !isFilesEqual {
		t.Errorf("Files are not equal")
	}
	t.Log("Files are equal")
}

func TestSendReadFile(t *testing.T) {
	// start a goroutine to read the file first since its the server
	var wg sync.WaitGroup
	saveFilepath := "..\\test_files\\tcp_net_tests\\read_file_test.txt"
	fileBeingSent := "..\\test_files\\tcp_net_tests\\send_file_test.txt"

	go func() {
		wg.Add(1)
		readFileHelper(t, saveFilepath, fileBeingSent)
		wg.Done()
	}()
	time.Sleep(500 * time.Millisecond) // wait for the server to start listening

	// now send the file
	sendFilePath := "..\\test_files\\tcp_net_tests\\send_file_test.txt"
	sendFileHelper(t, sendFilePath)

	wg.Wait() // just wait for the goroutine to finish
}

func TestSendFileLarge(t *testing.T) {

}

func TestReadFileLarge(t *testing.T) {

}
