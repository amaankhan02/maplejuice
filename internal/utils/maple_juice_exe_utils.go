package utils

import (
	"os"
	"strconv"
	"strings"
)

func getArgsSQLJoin() (string, int) {
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

/*
Used for maple/juice SQL exes
 */
func GetColumnIndex(schema_line string, column string) int {
	columns := strings.Split(schema_line, " ")

	index := 0
	for i, col_in_line := range columns {
		if col_in_line == column {
			index = i
		}
	}
	return index
}
