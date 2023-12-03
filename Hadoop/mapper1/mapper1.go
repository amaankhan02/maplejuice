package mapper1

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func Mapper1(scanner *bufio.Scanner, x string) {
	// get command line arguments

	fmt.Fprintln(os.Stderr, "Inside MAPPER function")
	column_index_interconne := 10
	column_index_detection := 9

	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, ",")

		interconne_val := words[column_index_interconne]
		detection_val := words[column_index_detection]

		if interconne_val == x {
			fmt.Fprintln(os.Stderr, "Debug: Mapper emitting key-value pair:", detection_val, 1)
			fmt.Printf("%s\t%d\n", detection_val, 1)
		}
	}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	x := os.Getenv("X")

	Mapper1(scanner, x)
}
