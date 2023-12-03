package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
)

func MapleDemoPhase2(scanner *bufio.Scanner, num_lines int) map[string][]string {
	// create map for each "null" -> line
	null_to_line := make(map[string][]string)
	for i := 0; i < num_lines && scanner.Scan(); i++ {
		line := scanner.Text()

		fmt.Println("null\t" + line) // already ends with "\n" because line includes that from previous reduce
	}

	return null_to_line
}

// get X value
func getArgs() int {
	// get the command line arg which tells you the number of lines
	num_lines_string := os.Args[1]
	num_lines, _ := strconv.Atoi(num_lines_string) // Convert the argument to an integer

	return num_lines
}

// actual executable
func main() {
	num_lines := getArgs()
	MapleDemoPhase2(bufio.NewScanner(os.Stdin), num_lines)
}
