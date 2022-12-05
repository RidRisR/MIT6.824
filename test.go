package main

import (
	"fmt"
)

func main() {

	ch := make(chan int, 100)

	for i := 0; i < 40; i++ {
		if i%2 == 0 {
			ch <- 1
		} else {
			ch <- 0
		}
	}
	m := 0
	n := 0
	flag := false
	for {
		select {
		case i := <-ch:
			if i == 0 {
				m++
			} else {
				n++
			}
		default:
			flag = true
		}
		if flag {
			break
		}
	}
	fmt.Printf("%v,%v", m, n)
}
