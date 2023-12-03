package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

func JuicePhase2(scanner *bufio.Scanner) {
	detection_to_count, total := parse(scanner)

	for detection, count := range detection_to_count {
		percentage := (float32(count) / total) * 100.0

		// convert percentage to string
		percentage_string := strconv.FormatFloat(float64(percentage), 'f', -1, 32)

		fmt.Println(detection + "," + percentage_string)
	}
}

func parse(scanner *bufio.Scanner) (map[string]int, float32) {
	total := 0

	detection_to_count := make(map[string]int)

	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")

		val := fields[1]

		detection_count := strings.Split(val, ",")

		detection := detection_count[0]
		count_string := detection_count[1]
		count, _ := strconv.Atoi(count_string)

		detection_to_count[detection] = count

		total += count
	}

	return detection_to_count, float32(total)
}

func main() {
	JuicePhase2(bufio.NewScanner(os.Stdin))
}
