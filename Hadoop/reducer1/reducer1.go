package reducer1

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func Reducer1(scanner *bufio.Scanner) {
	detection_to_counts := make(map[string]int)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",")

		detection_val := fields[0]
		count_string := fields[1]
		count, _ := strconv.Atoi(count_string)

		//TODO: see if we want to exclude the " " & ""
		detection_to_counts[detection_val] += count
	}

	// EMIT VALUES
	for key, val := range detection_to_counts {
		fmt.Printf("%s,%d\n", key, val)
	}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	Reducer1(scanner)
}
