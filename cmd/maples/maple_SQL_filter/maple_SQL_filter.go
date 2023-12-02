package maples_exe_sql_filter

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// this script will generate the key value pairs you need to use for filtering based on SQL command
// SELECT ALL FROM dataset WHERE <regex>

func MapleSQLFilter(scanner *bufio.Scanner, regex_string string, column string, num_lines int) map[string]string {
	// loop through the lines in the map task
	// first line is always the schema
	// first column is always the id
	// for every line, only look at the element in the specified column
	// check if that specified column matches the regex expression
	// if it does, we output (key - id, val - line)

	id_to_row := make(map[string]string)

	// compile regex expression
	regex, err := regexp.Compile(regex_string)
	if err != nil {
		log.Fatal("Error compiling regex: ", err)
	}

	schema := ""
	column_index := 0

	// loop through all lines from stdin
	for i := 0; i < num_lines && scanner.Scan(); i++ {

		// Define schema from first line & get column index
		if i == 0 {
			schema = scanner.Text()

			// get index of column you are looking for
			column_index = GetColumnIndex(schema, column)
			continue
		}

		// get each line
		line := scanner.Text()

		// get list of words from the line
		words := strings.Fields(line)

		field := words[column_index]

		if regex.MatchString(field) {
			id_to_row[line] = ""
		}
	}
	return id_to_row
}

func getArgsSQLFilter() (string, string, int) {
	// get the command line arg which tells you the number of lines
	// SELECT ALL FROM dataset WHERE COL = <regex> num_lines ?
	// need to know:
	// number of lines
	// column
	// regex

	// TODO: update with index after asking amaan
	regex := os.Args[0]
	column := os.Args[1]

	num_lines_string := os.Args[2]                 // QUESTION =: should I add a 1 to this to account for the schema being the first line every time
	num_lines, _ := strconv.Atoi(num_lines_string) // Convert the argument to an integer
	return regex, column, num_lines
}

func PrintKeyValPairsSQLFilter(kv_pairs map[string]string) {
	for key, val := range kv_pairs {
		fmt.Printf("%s,%s\n", key, val)
	}
}

func main() {
	regex, column, num_lines := getArgsSQLFilter()
	id_to_row := MapleSQLFilter(bufio.NewScanner(os.Stdin), regex, column, num_lines)
	PrintKeyValPairsSQLFilter(id_to_row)
}
