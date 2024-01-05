package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func Mapper2(scanner *bufio.Scanner) {
	for scanner.Scan() {
		line := scanner.Text()

		fields := strings.Split(line, "\t")
		new_line := fields[0] + "," + fields[1]

		fmt.Println("null\t" + new_line) // already ends with "\n" because line includes that from previous reduce
	}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	Mapper2(scanner)
}
