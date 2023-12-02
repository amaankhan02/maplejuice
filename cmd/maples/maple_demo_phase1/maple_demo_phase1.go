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
func MapleDemoPhase1(scanner *bufio.Scanner, num_lines int, X string) map[string]int {
	// create map for each word -> word_count
	detection_val_to_count := make(map[string]int)

	schema := ""
	column_index_interconne := 0
	column_index_detection := 0
	columnInterconne := "Interconne" // looking for this column
	columnDetection := "Detection_"

	// loop through all lines from stdin
	for i := 0; i < num_lines && scanner.Scan(); i++ {

		// Define schema from first line & get column index
		if i == 0 {
			schema = scanner.Text()

			column_index_interconne = mj.GetColumnIndex(schema, columnInterconne)
			column_index_detection = mj.GetColumnIndex(schema, columnDetection)

			continue
		}

		// get each line
		line := scanner.Text()

		// get list of words from the line
		// TODO: is this how you would do this from a csv file?
		words := strings.Fields(line)

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

// get X value
func getArgs() (int, string) {
	// get the command line arg which tells you the number of lines
	num_lines_string := os.Args[1]
	num_lines, _ := strconv.Atoi(num_lines_string) // Convert the argument to an integer

	X := os.Args[2]
	return num_lines, X
}

func PrintKeyValuePairs(kv_pairs map[string]int) {
	// print out all key, val pairs
	for key, val := range kv_pairs {
		fmt.Printf("%s,%d\n", key, val)
	}
}

// actual executable
func main() {
	num_lines, X := getArgs()
	detection_val_to_count := MapleDemoPhase1(bufio.NewScanner(os.Stdin), num_lines, X)
	PrintKeyValuePairs(detection_val_to_count)
}
