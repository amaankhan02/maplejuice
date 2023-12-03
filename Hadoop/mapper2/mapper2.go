package mapper2

import (
	"bufio"
	"fmt"
	"os"
)

func Mapper2(scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Println("null\t" + line) // already ends with "\n" because line includes that from previous reduce
	}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	Mapper2(scanner)
}
