package utils

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

type MapleSQLJoin1Value struct {
	Field   string
	Dataset string
}

/*
Used for maple/juice SQL exes
*/
func GetColumnIndex(schema_line string, column string) int {
	columns := strings.Split(schema_line, ",")

	index := 0
	for i, col_in_line := range columns {
		if col_in_line == column {
			index = i
		}
	}
	return index
}

func GetArgsMaple() (*os.File, int, int, string) {
	inputFilepath := os.Args[1]
	startingLine, _ := strconv.Atoi(os.Args[2])
	numLines, _ := strconv.Atoi(os.Args[3])

	inputFile, fileErr := os.OpenFile(inputFilepath, os.O_RDONLY, 0744)
	if fileErr != nil {
		log.Fatalln("Failed to open input file")
	}

	// get X value which is val in "Interconne" to search for
	X := os.Args[4]

	return inputFile, startingLine, numLines, X
}

func GetArgsJuice() *os.File {
	inputFilepath := os.Args[1]
	inputFile, fileErr := os.OpenFile(inputFilepath, os.O_RDONLY, 0744)
	if fileErr != nil {
		os.Exit(3)
		//log.Fatalln("Failed to open input file")
	}
	return inputFile
}

func GetArgsSQLJoin() (string, int) {
	// get the command line arg which tells you the number of lines
	// SELECT ALL FROM dataset WHERE COL = <regex> num_lines ?
	// need to know:
	// number of lines
	// column
	// regex

	// TODO: update with index after asking amaan
	column := os.Args[0]

	num_lines_string := os.Args[1]                 // QUESTION =: should I add a 1 to this to account for the schema being the first line every time
	num_lines, _ := strconv.Atoi(num_lines_string) // Convert the argument to an integer
	return column, num_lines
}

func PrintKeyValPairsSQLFilter(kv_pairs map[string]string) {
	for key, val := range kv_pairs {
		fmt.Printf("%s\t%s\n", key, val)
	}
}

func PrintKeyValPairsSQLJoin(kv_pairs map[string]MapleSQLJoin1Value) {
	for key, val := range kv_pairs {
		fmt.Printf("%s,%v\n", key, val)
	}
}

/*
MoveFilePointerToLineNumber
StartingLine is 1-indexed. Moves the file pointer to the start of that line
*/
func MoveFilePointerToLineNumber(fileScanner *bufio.Scanner, startingLine int) {
	for k := 1; k < startingLine; k++ {
		if fileScanner.Scan() == false {
			log.Fatalln("Could not reach startingLine passed in. Unable to run maple exe")
		}
	}
}

func PrintKeyValuePairs(kv_pairs map[string]int) {
	// print out all key, val pairs
	for key, val := range kv_pairs {
		fmt.Printf("%s\t%d\n", key, val)
	}
}
