package juice_exe

import (
	"bufio"
	"strconv"
	"strings"
)

func JuicePhase2(scanner *bufio.Scanner) map[string]float32 {
	detection_to_percentage := make(map[string]float32)
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

//func MainJuiceSQL() {
//	detection_to_percentage := JuicePhase2(bufio.NewScanner(os.Stdin))
//	maple_demo_phase2.PrintKeyValuePairs(detection_to_percentage)
//}
