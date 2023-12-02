package maples_exe_sql_join_phase_1

import (
	"bufio"
	mj "cs425_mp4/internal/maplejuice_exe"
	"os"
	"strings"
)

// go through every line
// check if the component from D1 equals component from D2
// if true:
// Concatenate D1 line & D2 line
// Add this to
// if it does you want to add

// processes each of the datasets 1
func MapleSQLJoin1(scanner *bufio.Scanner, column string, num_lines int) map[string]mj.MapleSQLJoin1Value {
	row_to_field := make(map[string]mj.MapleSQLJoin1Value)
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
			column_index = mj.GetColumnIndex(schema, column)
			continue
		}

		// get list of words from the line
		words := strings.Fields(line)

		field := words[column_index]

		row_to_field[line] = mj.MapleSQLJoin1Value{
			Field:   field,
			Dataset: "D1",
		}
	}

	return row_to_field
}

func main() {
	column, num_lines := mj.GetArgsSQLJoin()
	row_to_field := MapleSQLJoin1(bufio.NewScanner(os.Stdin), column, num_lines)
	mj.PrintKeyValPairsSQLJoin(row_to_field)
}
