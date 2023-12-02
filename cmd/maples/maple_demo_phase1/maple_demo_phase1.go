package maple_demo_phase1

import (
	"bufio"
	mj "cs425_mp4/internal/maplejuice_exe"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// TODO: need to see how to parse the empty cells in CSV file, right now replacing with ""
func MapleDemoPhase1(scanner *bufio.Scanner, num_lines int, X string, schema string) map[string]int {
	// create map for each word -> word_count
	detection_val_to_count := make(map[string]int)

	columnInterconne := "Interconne" // looking for this column
	columnDetection := "Detection_"

	// define schema
	column_index_interconne := mj.GetColumnIndex(schema, columnInterconne)
	column_index_detection := mj.GetColumnIndex(schema, columnDetection)

	// loop through all lines from stdin
	for i := 0; i < num_lines && scanner.Scan(); i++ {
		// get each line
		line := scanner.Text()

		words := strings.Split(line, ",")

		// val in interconne & detection column
		interconne_val := words[column_index_interconne]
		detection_val := words[column_index_detection]

		// if the interconne val matches
		if interconne_val == X {
			_, exists := detection_val_to_count[detection_val]

			if exists {
				detection_val_to_count[detection_val] += 1
			} else {
				detection_val_to_count[detection_val] = 1
			}
		}
	}
	return detection_val_to_count
}

// TODO: adjust arg values
func getArgs() (int, string, string) {
	// get the command line arg which tells you the number of lines
	num_lines_string := os.Args[1]
	num_lines, _ := strconv.Atoi(num_lines_string) // Convert the argument to an integer

	// get X value which is val in "Interconne" to search for
	X := os.Args[2]

	// get the schema
	schema := os.Args[3]

	return num_lines, X, schema
}

func PrintKeyValuePairs(kv_pairs map[string]int) {
	// print out all key, val pairs
	for key, val := range kv_pairs {
		fmt.Printf("%s,%d\n", key, val)
	}
}

// actual executable
func main() {
	num_lines, X, schema := getArgs()
	detection_val_to_count := MapleDemoPhase1(bufio.NewScanner(os.Stdin), num_lines, X, schema)
	PrintKeyValuePairs(detection_val_to_count)
}
