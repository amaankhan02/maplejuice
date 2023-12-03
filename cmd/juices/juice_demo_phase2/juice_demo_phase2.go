package juice_demo_phase2

import (
	"bufio"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
)

func JuicePhase2(scanner *bufio.Scanner) map[string]float32 {
	// get "null" -> [[]]

	detection_to_count, total := parse(scanner)
	detection_to_percentage := make(map[string]float32)

	for detection, count := range detection_to_count {
		percentage := (float32(count) / total) * 100.0

		// PRINT OUT RESULT
		// TODO: how to print out properly
		fmt.Println(detection, percentage)
	}

	for i := 0; scanner.Scan(); i++ {
		line := scanner.Text()

		key_val := strings.Split(line, ",")
		detection := key_val[0]
		percentage_string := key_val[1]
		percentage, _ := strconv.ParseFloat(percentage_string, 32)

		detection_to_percentage[detection] = float32(percentage)
	}

	return detection_to_percentage
}

func parse(scanner *bufio.Scanner) (map[string]int, float32) {
	detection_to_count := make(map[string]int)
	total := 0

	for i := 0; scanner.Scan(); i++ {
		line := scanner.Text()

		value := extractValue(line)
		tuples := getTuples(value)

		for _, tuple := range tuples {
			key_val := strings.Split(tuple, ",")
			detection := key_val[0]
			count_string := key_val[1]
			count, _ := strconv.Atoi(count_string)

			_, exists := detection_to_count[detection]
			if exists {
				detection_to_count[detection] += count
			} else {
				detection_to_count[detection] = count
			}

			// TODO: if detection == "" or " ", do not add it to total and do not add it to the dictionary?
			total += count
		}
	}
	return detection_to_count, float32(total)
}

func getTuples(value string) []string {
	regex := regexp.MustCompile(`\((.*?)\)`)

	matches := regex.FindAllStringSubmatch(value, -1)

	var tuples []string
	for _, match := range matches {
		tuples = append(tuples, match[1])
	}

	return tuples
}

func extractValue(line string) string {
	startBracketIndex := strings.Index(line, "[")

	if startBracketIndex == -1 || (startBracketIndex > len(line)-1) {
		log.Fatal("Invalid Input Format")
	}

	// remove brackets as well
	value := line[startBracketIndex+1 : len(line)-1]

	return value
}

func main() {
	//detection_to_percentage := JuicePhase2(bufio.NewScanner(os.Stdin))
	//maple_demo_phase2.PrintKeyValuePairs(detection_to_percentage)
}
