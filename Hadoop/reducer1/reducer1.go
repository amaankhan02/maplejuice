package reducer1

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func reducer1() {
	scanner := bufio.NewScanner(os.Stdin)

	detection_to_counts := make(map[string]int)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ",")

		detection_val := fields[0]
		count_string := fields[1]
		count, _ := strconv.Atoi(count_string)

		_, exists := detection_to_counts[detection_val]

		if exists {
			detection_to_counts[detection_val] += count
		} else {
			detection_to_counts[detection_val] = count
		}
	}

	// EMIT VALUES
	for key, val := range detection_to_counts {
		fmt.Printf("%s,%d\n", key, val)
	}
}

func main() {
	reducer1()
}
