package mapper1

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func mapper1() {
	scanner := bufio.NewScanner(os.Stdin)

	// get command line arguments
	x := os.Getenv("X")
	schema := os.Getenv("schema")

	columnInterconne := "Interconne" // looking for this column
	columnDetection := "Detection_"

	// define schema
	column_index_interconne := GetColumnIndex(schema, columnInterconne)
	column_index_detection := GetColumnIndex(schema, columnDetection)

	for scanner.Scan() {
		line := scanner.Text()
		words := strings.Split(line, ",")

		interconne_val := words[column_index_interconne]
		detection_val := words[column_index_detection]

		if interconne_val == x {
			fmt.Printf("%s,%d\n", detection_val, 1)
		}
	}
}

func GetColumnIndex(schema_line string, column string) int {
	columns := strings.Split(schema_line, ",")

	index := 0
	for i, col_in_line := range columns {
		if col_in_line == column {
			index = i
		}
	}
	return index
}

func main() {
	mapper1()
}
