package maples_exe_word_count

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// this script will generate the key value pairs you need to use for counting number of words
// maple_juice -> 1 map task

func MapleWordCount(scanner *bufio.Scanner, num_lines int) map[string]int {
	// create map for each word -> word_count
	word_to_word_count := make(map[string]int)

	// loop through all lines from stdin
	for i := 0; i < num_lines && scanner.Scan(); i++ {
		// get each line
		line := scanner.Text()

		// get list of words from the line
		words := strings.Fields(line)

		// loop through all the words & update dictionary
		for _, word := range words {
			_, exists := word_to_word_count[word]

			if exists {
				word_to_word_count[word] += 1
			} else {
				word_to_word_count[word] = 1
			}
		}
	}
	return word_to_word_count
}

// get the number of lines
func getArgs() int {
	// get the command line arg which tells you the number of lines
	num_lines_string := os.Args[1]
	num_lines, _ := strconv.Atoi(num_lines_string) // Convert the argument to an integer
	return num_lines
}

func PrintKeyValuePairs(kv_pairs map[string]int) {
	// print out all key, val pairs
	for key, val := range kv_pairs {
		fmt.Printf("%s,%d\n", key, val)
	}
}

// actual executable
func main() {
	num_lines := getArgs()
	word_to_word_count := MapleWordCount(bufio.NewScanner(os.Stdin), num_lines)
	PrintKeyValuePairs(word_to_word_count)
}
