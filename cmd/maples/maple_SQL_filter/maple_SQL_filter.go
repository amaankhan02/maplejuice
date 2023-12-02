package maples_exe_sql_filter

import (
	"bufio"
	mj "cs425_mp4/internal/maplejuice_exe"
	"log"
	"os"
	"regexp"
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

func main() {
	regex, num_lines, schema := mj.GetArgsSQLFilter()
	id_to_row := MapleSQLFilter(bufio.NewScanner(os.Stdin), regex, num_lines, schema)
	mj.PrintKeyValPairsSQLFilter(id_to_row)
}
