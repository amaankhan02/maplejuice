package main

import (
	"bufio"
	"cs425_mp4/internal/maplejuice_exe"
	"fmt"
	"log"
	"os"
	"regexp"
)

// this script will generate the key value pairs you need to use for filtering based on SQL command
// SELECT ALL FROM dataset WHERE <regex>
func MapleSQLFilter(scanner *bufio.Scanner, starting_line_number int, num_lines int, regex_string string) map[string]string {
	id_to_row := make(map[string]string)

	// startingLine is 1-indexed. Move file pointer to startingLine
	maplejuice_exe.MoveFilePointerToLineNumber(scanner, starting_line_number)

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
			id_to_row[line] = "NULL" // NOTE: this will result in an extra comma at the end of the line
		}
	}
	return id_to_row
}

func main() {
	input_file, starting_line_number, num_lines, regex := maplejuice_exe.GetArgsMaple()
	defer input_file.Close()

	scanner := bufio.NewScanner(input_file)
	id_to_row := MapleSQLFilter(scanner, starting_line_number, num_lines, regex)
	_, _ = fmt.Fprintf(os.Stderr, "id_to_row: %v\n", id_to_row)

	maplejuice_exe.PrintKeyValPairsSQLFilter(id_to_row)
}
