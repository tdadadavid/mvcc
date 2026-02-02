package main

import (
	"fmt"
	"os"
	"slices"
)

func assert(b bool, msg string) {
	if !b {
		panic(msg)
	}
}

func assertEq[C comparable](a, b C, prefix string) {
	if a != b {
		fmt.Printf("%s %v != %v", prefix, a, b)
	}
}

var DEBUG = slices.Contains(os.Args, "--debug")

func debug(a ...any) {
	if DEBUG {
		return
	}
	args := append([]any{"[DEBUG]"}, a)
	fmt.Println(args...)
}

func main() {
}
