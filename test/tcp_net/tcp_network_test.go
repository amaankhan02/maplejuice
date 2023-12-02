package tcp_net

import (
	"cs425_mp4/internal/tcp_net"
	"cs425_mp4/internal/utils"
	"net"
	"testing"
)

func TestSendFileSmall(t *testing.T) {
	filepath := "cs425_mp4/test/test_files/tcp_net_tests/send_file_test.txt"
	filesize := utils.GetFileSize(filepath)

	// dial to loopback server on port 8080
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	err = tcp_net.SendFile(filepath, conn, filesize)
	if err != nil {
		t.Errorf("Error sending file: %s", err)
	}
	t.Log("File sent successfully")
}

/*
This function is used in conjunction with TestSendFileSmall to test the
receiving end to see if it got the file correctly
*/
func TestSendFileReceiver(t *testing.T) {

}

func TestSendFileLarge(t *testing.T) {

}

type TestHandler struct{}

func (h *TestHandler) HandleTCPServerConnection(conn net.Conn) {
	filepath := "cs425_mp4/test/test_files/tcp_net_tests/read_file_test.txt"
	filesize := utils.GetFileSize(filepath)
	err := tcp_net.ReadFile(filepath, conn, filesize)
	if err != nil {
		t.Errorf("Error reading file: %s", err)
	}
	t.Log("File read successfully")
}

func TestReadFileSmall(t *testing.T) {
	listener, err := net.Listen("tcp", ":"+"8080")
	if err != nil {
		t.Errorf("Error listening: %s", err)
	}

	conn, err := listener.Accept()
	if err != nil {
		t.Errorf("Error accepting: %s", err)
	}
	saveFilepath := "cs425_mp4/test/test_files/tcp_net_tests/read_file_test.txt"
	fileBeingSent := "cs425_mp4/test/test_files/tcp_net_tests/send_file_test.txt"
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

func TestReadFileLarge(t *testing.T) {

}
