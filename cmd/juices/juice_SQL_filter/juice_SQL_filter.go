package main

import (
	"bufio"
	"cs425_mp4/internal/maplejuice_exe"
	"fmt"
)

// This Juice just outputs the key value it gets, does not do anything, this is used for both FILTER & JOIN
func JuiceSQL(scanner *bufio.Scanner) {
	// output all the values
	// should only be one line cause one key only had one value

	for i := 0; scanner.Scan(); i++ {
		line := scanner.Text()
		fmt.Println(line) // TODO: might be an extra comma at the end because it was (key, - )
	}
}

func main() {
	inputFile := maplejuice_exe.GetArgsJuice()
	defer inputFile.Close()
	JuiceSQL(bufio.NewScanner(inputFile))
}
