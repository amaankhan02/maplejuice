package tcp_net

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
)

const MESSAGE_SIZE_BYTES = 4 // number of bytes used in the protocol to define the size of the message
const BUFFER_SIZE = 4096

/*
Protocol Format for SDFS:
	[type][size][data]

Format for sending a file:
	[type][size][response data][file data]
	* the [size] represents size of the response data struct
	* the response data struct will contain a .size variable representing the size of [file data]
*/

/*
[type] is a byte representing the message type. There are 2^8=256 different kinds of message types since

	this is 1 byte long
*/
func SendMessageType(message_type byte, conn net.Conn) error {
	var err error
	// write [type]
	msg_type := make([]byte, 1) // convert the message_type from a byte to a byte array (b/c that's what Write() takes)
	msg_type[0] = message_type
	_, err = conn.Write(msg_type)
	if err != nil {
		return err
	}

	return nil
}

/*
Send message data. Pass in the array of bytes of data. It will calcualte the length of the data
and send that as well preceding the actual data itself.

Format: [size][data]

	[size] is the size of the data represented in a binary format - 4 Byte big-endian
	[data] is a []byte of the serialize GrepQuery/GrepOutput object gquery (use grep.SerializeGrepOutput())
*/
func SendMessageData(data []byte, conn net.Conn) error {
	var err error

	// write [size]
	size := len(data)
	err = sendMessageSize(size, conn, MESSAGE_SIZE_BYTES)
	if err != nil {
		return err
	}

	// write [data]
	_, err = conn.Write(data)
	if err != nil {
		return err
	}

	return nil
}

/*
Reads a file and sends it over a TCP connection in a buffered format
so that it does NOT read the entire file into memory. But rather, it
sends as a stream. Always use this function when sending a file over TCP
*/
func SendFile(filepath string, conn net.Conn, filesize int64) error {
	// open the file for reading
	fmt.Println("Inside SendFile() - filepath: ", filepath)
	f, err := os.OpenFile(filepath, os.O_RDONLY, 0666)
	if err != nil {
		fmt.Println("Failed to open file for reading")
		return err
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, BUFFER_SIZE)
	n_written, copy_err := io.Copy(conn, reader) // reads until the EOF
	if copy_err != nil {
		return copy_err
	}
	fmt.Println("Actual number of bytes copied from file: ", n_written)
	if n_written < filesize {
		fmt.Println("Failed to copy entire file into connection!")
		return errors.New("failed to copy entire file into connection")
	} else {
		return nil
	}
}

/*
Read the message type (1 byte long)
*/
func ReadMessageType(reader *bufio.Reader) (byte, error) {
	var buff []byte
	//var n_read int
	var err error

	// read [type]
	buff = make([]byte, 1) // 1 because the message type is just 1 byte long

	//TODO: n_read was giving compiler error for never being used.......
	//n_read, err = io.ReadFull(sreader, buff)
	_, err = io.ReadFull(reader, buff)
	if err != nil {
		return 0, err
	}

	return buff[0], nil
}

/*
Read request from connection. Reads the message size and correctly gets all the []byte of data
and returns it. Caller is expected to deserialize this []byte of data as this function does not
do that.

# Returns an error of io.EOF or io.ErrUnexpectedEOF

Format: [size][binary data]

	[size] = size of the binary data (MESSAGE_SIZE_BYTES long)
	[binary data] = binary data of [size] length
*/
func ReadMessageData(reader *bufio.Reader) ([]byte, error) {
	var buff []byte
	var n_read int
	var err error

	// read [size]
	n_read, err = readMessageSize(reader, MESSAGE_SIZE_BYTES)

	if err != nil {
		return nil, err
	}

	buff = make([]byte, n_read)

	// read [binary data]
	// ReadFull() reads exactly len(buff) bytes from reader into buff
	// returns the number of bytes copied, and an error if fewer than len(buff) bytes were read
	// the error is io.EOF only if no bytes were read. otherwise its an ErrUnexpectedEOF
	_, err = io.ReadFull(reader, buff)
	if err != nil { // fewer than len(buff) bytes were read, or none at all
		return nil, err
	}

	return buff, nil
}

/*
Reads a file from the TCP connection and writes it to 'filepath'.
'filesize' represents the amount of data from the connection socket to read

If the save_filepath does not exist, it will create it.
If the save_filepath does exist, it will truncate the file to be empty and then write
the contents
*/
func ReadFile(save_filepath string, conn net.Conn, filesize int64) error {
	// open the file for writing
	fmt.Println("Inside ReadFile() - save_filepath: ", save_filepath)
	fmt.Println("Expected filesize: ", filesize)
	f, err := os.OpenFile(save_filepath, os.O_WRONLY|os.O_CREATE, 0744)
	if err != nil {
		fmt.Println("Failed to open file for writing into")
		return err
	}
	defer f.Close()

	reader := bufio.NewReaderSize(conn, BUFFER_SIZE)
	//var b_copied int64 = 0
	//var amt int64 = BUFFER_SIZE

	//for b_copied < filesize {
	//	if b_copied+amt > filesize {
	//		amt = filesize - b_copied
	//	} else {
	//		amt = BUFFER_SIZE
	//	}
	//	n, copy_err := io.CopyN(f, reader, amt)
	//	if copy_err != nil {
	//		fmt.Println("ACTUAL NUM BYTES COPIED FROM connection: ", b_copied)
	//		fmt.Println("Failed to copy file from connection into local disk! - ", copy_err)
	//		return copy_err
	//	}
	//
	//	b_copied += n
	//}

	// TODO: change to just read until the EOF...
	n_read, copy_err := io.Copy(f, reader)
	if copy_err != nil {
		fmt.Println("(error) ACTUAL NUM BYTES COPIED FROM connection into file: ", n_read)
		fmt.Println("error: ", copy_err.Error())
		return copy_err
	}
	fmt.Println("(no error) ACTUAL NUM BYTES COPIED FROM connection into file: ", n_read)

	return nil
}

func ReadFile2(save_filepath string, conn net.Conn, filesize int64) error {
	// open the file for writing
	fmt.Println("####Inside ReadFile2() - save_filepath: ", save_filepath)
	fmt.Println("Expected filesize: ", filesize)
	f, err := os.OpenFile(save_filepath, os.O_WRONLY|os.O_CREATE, 0744)
	if err != nil {
		fmt.Println("Failed to open file for writing into")
		return err
	}
	defer f.Close()

	reader := bufio.NewReaderSize(conn, BUFFER_SIZE)
	var buff bytes.Buffer

	n_read, copy_err := io.Copy(&buff, reader)
	if copy_err != nil {
		fmt.Println("(error) ACTUAL NUM BYTES COPIED FROM connection into file: ", n_read)
		fmt.Println("error: ", copy_err.Error())
		return copy_err
	}
	fmt.Println("(no error) ACTUAL NUM BYTES COPIED FROM connection into file: ", n_read)

	n_written, write_err := f.Write(buff.Bytes())
	if write_err != nil {
		fmt.Println("Failed to write to file: ", write_err.Error())
		fmt.Println("Wrote this many bytes: ", n_written)
		return write_err
	}
	fmt.Println("Wrote this many bytes to file: ", n_written)

	return nil
}

/*
Helper function to read just the message size from the connection
*/
func readMessageSize(reader *bufio.Reader, messageSizeBytes int) (int, error) {
	if messageSizeBytes != 4 && messageSizeBytes != 8 {
		return 0, errors.New("Invalid argument for messageSizeBytes - must be either equal to 4 or 8")
	}

	buff := make([]byte, messageSizeBytes)
	_, err := io.ReadFull(reader, buff)
	if err != nil {
		return 0, err
	}

	if messageSizeBytes == 4 {
		return int(binary.BigEndian.Uint32(buff)), nil
	} else {
		return int(binary.BigEndian.Uint64(buff)), nil
	}
}

/*
Helper function to send just the message size as a MESSAGE_SIZE_BYTES number (like a 4byte number)
in big-endian format
*/
func sendMessageSize(base10Number int, conn net.Conn, messageSizeBytes int) error {
	size := make([]byte, MESSAGE_SIZE_BYTES)
	if messageSizeBytes == 4 {
		binary.BigEndian.PutUint32(size, uint32(base10Number))
	} else if messageSizeBytes == 8 {
		binary.BigEndian.PutUint64(size, uint64(base10Number))
	}

	_, err := conn.Write(size) // _ is the number of bytes sent

	if err != nil {
		return err
	}

	return nil
}
