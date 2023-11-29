package juice_exe

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func JuiceSQLFilter(scanner *bufio.Scanner) []string {
	// output all the values

	// should only be one line cause one key only had one value

	var matched_lines []string
	for i := 0; scanner.Scan(); i++ {
		line := scanner.Text()

		key_val := strings.Split(line, ",")

		matched_lines = append(matched_lines, key_val[1]) // QUESTION: do I need a comma like ",val" to indicate there was no key?
	}

	return matched_lines
}

func PrintSliceString(stringslice []string) {

	for _, val := range stringslice {
		fmt.Println(val)
	}
}

func MainJuiceSQLFilter() {
	matched_lines := JuiceSQLFilter(bufio.NewScanner(os.Stdin))
	PrintSliceString(matched_lines)
}
