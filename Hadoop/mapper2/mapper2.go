package mapper2

import (
	"bufio"
	"fmt"
	"os"
)

func mapper2() {

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println("null\t" + line) // already ends with "\n" because line includes that from previous reduce
	}
}

func main() {
	mapper2()
}
