package main

import (
	"bufio"
	//maples_exe_word_count "cs425_mp4/cmd/maple_word_count"
	"log"
	"strconv"
	"strings"
)

func JuiceDemoTest1(scanner *bufio.Scanner) map[string]int {
	// loop through stdin and get every line

	detection_val_to_count := make(map[string]int)
	for i := 0; scanner.Scan(); i++ {
		line := scanner.Text()

		key_val := strings.Split(line, ",")

		// get the word & word_count from line
		detection := key_val[0]
		detection_count_string := key_val[1]
		detection_count, err := strconv.Atoi(detection_count_string)

		if err != nil {
			log.Fatal("Error in converting word_count from string to integer")
		}

		// if word already exists in map, add word_count to the value
		_, exists := detection_val_to_count[detection]

		if exists {
			detection_val_to_count[detection] += detection_count
		} else {
			detection_val_to_count[detection] = detection_count
		}
	}

	return detection_val_to_count
}

func main() {
	//detection_val_to_count := JuiceDemoTest1(bufio.NewScanner(os.Stdin))
	//maples_exe_word_count.PrintKeyValuePairs(detection_val_to_count)
}
