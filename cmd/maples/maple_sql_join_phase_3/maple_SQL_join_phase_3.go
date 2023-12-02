package maples_exe_sql_join_phase_3

import (
	"bufio"
	"log"
	"os"
	"strings"
	mj "cs425_mp4/internal/maplejuice_exe"
)

func MapleSQLJoin3(scanner *bufio.Scanner, column string, num_lines int) map[string]string {

	joined_list := make(map[string]string)

	D1, D2 := getDatasets(scanner, num_lines)

	for _, d1Element := range D1 {
		for _, d2Element := range D2 {

			key_val1 := strings.Split(d1Element, ",")
			key1 := key_val1[0]
			val1 := key_val1[1]
			words_from_val1 := strings.Fields(val1)
			field1 := words_from_val1[0]

			key_val2 := strings.Split(d2Element, ",")
			key2 := key_val2[0]
			val2 := key_val2[1]
			words_from_val2 := strings.Fields(val2)
			field2 := words_from_val2[0]
			
			if field1 == field2 {
				new_key := key1 + " " + key2
				joined_list[new_key] = ""
			}
		}
	}

	return joined_list
}

func getDatasets(scanner *bufio.Scanner, num_lines int) ([]string, []string) {
	var D1 []string
	var D2 []string

	// loop through all lines from MJ1 and MJ2 output
	for i := 0; i < num_lines && scanner.Scan(); i++ {
		// get each line
		line := scanner.Text()

		key_val := strings.Split(line, ",")

		val := key_val[1]

		// get list of words from the line
		words_from_val := strings.Fields(val) // splits by space

		data_set := words_from_val[1]

		if data_set == "D1" {
			D1 = append(D1, line)
		} else if data_set == "D2" {
			D2 = append(D2, line)
		} else {
			log.Fatal("There is no dataset for: ", line)
		}

	}

	return D1, D2
}

func main() {
	column, num_lines := mj.GetArgsSQLJoin()
	row_to_field := MapleSQLJoin3(bufio.NewScanner(os.Stdin), column, num_lines)
	mj.PrintKeyValPairsSQLFilter(row_to_field)
}
