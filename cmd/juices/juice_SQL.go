package juice_exe

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// This Juice just outputs the key value it gets, does not do anything, this is used for both FILTER & JOIN
func JuiceSQL(scanner *bufio.Scanner) []string {
	// output all the values
	// should only be one line cause one key only had one value

	// QUESTION: should this be a map instead that maps string to an empty string ""
	// because this is like outputting (key, -)
	var matched_lines []string
	for i := 0; scanner.Scan(); i++ {
		line := scanner.Text()

		key_val := strings.Split(line, ",")

		matched_lines = append(matched_lines, key_val[0])
	}

	return matched_lines
}

func PrintSliceString(stringslice []string) {
	for _, val := range stringslice {
		fmt.Println(val)
	}
}

func MainJuiceSQL() {
	matched_lines := JuiceSQL(bufio.NewScanner(os.Stdin))
	PrintSliceString(matched_lines)
}
