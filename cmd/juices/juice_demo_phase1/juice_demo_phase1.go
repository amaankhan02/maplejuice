package main

import (
	"bufio"
	"cs425_mp4/internal/maplejuice_exe"
	"fmt"
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

		detection_val_to_count[detection] += detection_count
	}

	return detection_val_to_count
}

func PrintKeyValuePairs(kv_pairs map[string]int) {
	for key, val := range kv_pairs {
		fmt.Printf("%s\t%d\n", key, val)
	}
}

func main() {
	inputFile := maplejuice_exe.GetArgsJuice()
	defer inputFile.Close()
	detection_val_to_count := JuiceDemoTest1(bufio.NewScanner(inputFile))
	PrintKeyValuePairs(detection_val_to_count)
}
