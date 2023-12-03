package main

import (
	"bufio"
	mj "cs425_mp4/internal/maplejuice_exe"
	"log"
	"os"
	"regexp"
	"strconv"
)

// this script will generate the key value pairs you need to use for filtering based on SQL command
// SELECT ALL FROM dataset WHERE <regex>
func MapleSQLFilter(scanner *bufio.Scanner, regex_string string, num_lines int, schema string) map[string]string {
	id_to_row := make(map[string]string)

	//compile regex expression
	regex, err := regexp.Compile(regex_string)
	if err != nil {
		log.Fatal("Error compiling regex: ", err)
	}

	// loop through all lines from stdin
	for i := 0; i < num_lines && scanner.Scan(); i++ {
		// get each line
		line := scanner.Text()

		// check if the regex exists in the line
		if regex.MatchString(line) {
			id_to_row[line] = "" // NOTE: this will result in an extra comma at the end of the line
		}
	}
	return id_to_row
}

// TODO: update index values of args
func GetArgsSQLFilter() (string, int, string) {
	// get the command line arg which tells you the number of lines
	// SELECT ALL FROM dataset WHERE COL = <regex> num_lines ?
	// need to know:
	// number of lines
	// column
	// regex

	// first arg -> num lines,
	// second arg -> column schema
	// third arg -> regex

	num_lines_string := os.Args[0]
	num_lines, _ := strconv.Atoi(num_lines_string) // Convert the argument to an integer

	schema := os.Args[1]

	regex := os.Args[3]
	//column := os.Args[1]

	return regex, num_lines, schema
}

func main() {
	regex, num_lines, schema := GetArgsSQLFilter()
	id_to_row := MapleSQLFilter(bufio.NewScanner(os.Stdin), regex, num_lines, schema)
	mj.PrintKeyValPairsSQLFilter(id_to_row)
}
