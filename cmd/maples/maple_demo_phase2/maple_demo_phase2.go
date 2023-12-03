// package main TODO: chang back to main
package maple_demo_phase2

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Tuple struct {
	DetectionWord string
	Count         int
}

func MapleDemoPhase2(scanner *bufio.Scanner, num_lines int) map[string][]string {
	// create map for each "null" -> line

	null_to_line := make(map[string][]string)
	for i := 0; i < num_lines && scanner.Scan(); i++ {
		line := scanner.Text()

		//key_val := strings.Split(line, ",")
		//detection_word := key_val[0]
		//count_string := key_val[1]
		//count, _  := strconv.Atoi(count_string)
		//
		//new_tuple := Tuple{detection_word,
		//	count}
		//

		//current_list := null_to_line["null"]
		//
		//val := "(" + line + ")"
		//
		//current_list = append(current_list, val)
		//
		//null_to_line["null"] = current_list

		fmt.Println("null\t" + line) // already ends with "\n" because line includes that from previous reduce
	}

	return null_to_line

	//detection_to_percentage := make(map[string]float32)
	//
	//total, detection_to_count := parse(scanner, num_lines)
	//
	//for detection, count := range detection_to_count {
	//	percentage := (float32(count) / total) * 100.0
	//	detection_to_percentage[detection] = percentage
	//}
	//
	//return detection_to_percentage
}

func parse(scanner *bufio.Scanner, num_lines int) (float32, map[string]float32) {
	total := 0
	detection_to_count := make(map[string]float32)
	for i := 0; i < num_lines && scanner.Scan(); i++ {
		line := scanner.Text()

		key_val := strings.Split(line, ",")
		detection := key_val[0]
		count_string := key_val[1]

		count, _ := strconv.Atoi(count_string)
		total += count

		detection_to_count[detection] = float32(count)
	}

	return float32(total), detection_to_count
}

// get X value
func getArgs() int {
	// get the command line arg which tells you the number of lines
	num_lines_string := os.Args[1]
	num_lines, _ := strconv.Atoi(num_lines_string) // Convert the argument to an integer

	return num_lines
}

func PrintKeyValuePairs(kv_pairs map[string][]string) {
	// print out all key, val pairs
	for key, val := range kv_pairs {
		fmt.Printf("%s,%v\n", key, val)
	}
}

// actual executable
func main() {
	num_lines := getArgs()
	MapleDemoPhase2(bufio.NewScanner(os.Stdin), num_lines)
	//PrintKeyValuePairs(detection_to_percentage)
}
