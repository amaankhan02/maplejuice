package maples_exe

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// go through every line
// check if the component from D1 equals component from D2
// if true:
// Concatenate D1 line & D2 line
// Add this to
// if it does you want to add

type MapleSQLJoin1Value struct {
	Field   string
	Dataset string
}

// processes each of the datasets 1
func MapleSQLJoin1(scanner *bufio.Scanner, column string, num_lines int) map[string]MapleSQLJoin1Value {
	row_to_field := make(map[string]MapleSQLJoin1Value)
	schema := ""
	column_index := 0

	// loop through all lines from d1
	for i := 0; i < num_lines && scanner.Scan(); i++ {
		// get each line
		line := scanner.Text()

		// Define schema from first line & get column index
		if i == 0 {
			schema = scanner.Text()

			// get index of column you are looking for
			column_index = GetColumnIndex(schema, column)
			continue
		}

		// get list of words from the line
		words := strings.Fields(line)

		field := words[column_index]

		row_to_field[line] = MapleSQLJoin1Value{
			field,
			"D1",
		}
	}

	return row_to_field
}

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

func PrintKeyValPairsSQLJoin(kv_pairs map[string]MapleSQLJoin1Value) {
	for key, val := range kv_pairs {
		fmt.Printf("%s,%v\n", key, val)
	}
}

func MainMapleSQLJoin() {
	column, num_lines := getArgsSQLJoin()
	row_to_field := MapleSQLJoin1(bufio.NewScanner(os.Stdin), column, num_lines)
	PrintKeyValPairsSQLJoin(row_to_field)
}
