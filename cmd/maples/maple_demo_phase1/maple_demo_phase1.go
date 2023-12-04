package main

import (
	"bufio"
	"cs425_mp4/internal/maplejuice_exe"
	"fmt"
	"strings"
)

func MapleDemoPhase1(scanner *bufio.Scanner, num_lines int, starting_line int, X string) map[string]int {
	// create map for each word -> word_count

	// startingLine is 1-indexed. Move file pointer to startingLine
	maplejuice_exe.MoveFilePointerToLineNumber(scanner, starting_line)

	detection_val_to_count := make(map[string]int)
	column_index_interconne := 10
	column_index_detection := 9

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

func PrintKeyValuePairs(kv_pairs map[string]int) {
	// print out all key, val pairs
	for key, val := range kv_pairs {
		fmt.Printf("%s\t%d\n", key, val)
	}
}

// actual executable
func main() {
	inputFile, starting_line, num_lines, X := maplejuice_exe.GetArgsMaple()
	defer inputFile.Close()

	detection_val_to_count := MapleDemoPhase1(bufio.NewScanner(inputFile), num_lines, starting_line, X)
	PrintKeyValuePairs(detection_val_to_count)
}
