package main

import (
	"bufio"
	"cs425_mp4/internal/utils"
	"log"
	"os"
	"strconv"
	"strings"
)

// given => key, val
// each key should be the same
// return => key, sum_val
func JuiceWordCount(scanner *bufio.Scanner) map[string]int {
	// loop through stdin and get every line

	word_to_word_count := make(map[string]int)
	for i := 0; scanner.Scan(); i++ {
		line := scanner.Text()

		key_val := strings.Split(line, ",")

		// get the word & word_count from line
		word := key_val[0]
		word_count_string := key_val[1]
		word_count, err := strconv.Atoi(word_count_string)

		if err != nil {
			log.Fatal("Error in converting word_count from string to integer")
		}

		// if word already exists in map, add word_count to the value
		_, exists := word_to_word_count[word]

		if exists {
			word_to_word_count[word] += word_count
		} else {
			word_to_word_count[word] = word_count
		}

		//fmt.Println(word_to_word_count)
	}

	return word_to_word_count
}

func getArgs() *os.File {
	inputFilepath := os.Args[1]
	inputFile, fileErr := os.OpenFile(inputFilepath, os.O_RDONLY, 0744)
	if fileErr != nil {
		os.Exit(3)
		//log.Fatalln("Failed to open input file")
	}
	return inputFile
}

func main() {
	inputFile := getArgs()
	defer inputFile.Close()
	//inputFilepath := os.Args[1]
	//fmt.Println(inputFilepath)

	wordToWordCount := JuiceWordCount(bufio.NewScanner(inputFile))
	utils.PrintKeyValuePairs(wordToWordCount)
}
