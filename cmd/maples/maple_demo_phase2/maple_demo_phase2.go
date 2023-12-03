package main

import (
	"bufio"
	"cs425_mp4/internal/maplejuice_exe"
	"fmt"
)

func MapleDemoPhase2(scanner *bufio.Scanner, starting_line int, num_lines int) map[string][]string {
	// create map for each "null" -> line

	// startingLine is 1-indexed. Move file pointer to startingLine
	maplejuice_exe.MoveFilePointerToLineNumber(scanner, starting_line)

	null_to_line := make(map[string][]string)
	for i := 0; i < num_lines && scanner.Scan(); i++ {
		line := scanner.Text()

		fmt.Println("null\t" + line) // already ends with "\n" because line includes that from previous reduce
	}

	return null_to_line
}

// actual executable
func main() {
	inputFile, starting_line, num_lines, _ := maplejuice_exe.GetArgsMaple()
	defer inputFile.Close()

	MapleDemoPhase2(bufio.NewScanner(inputFile), starting_line, num_lines)
}
