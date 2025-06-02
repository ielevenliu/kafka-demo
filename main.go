package main

import (
	"fmt"
	"github.com/ielevenliu/kafka-demo/consumer"
	"log/slog"
)

func main() {
	fmt.Println("hi")

	err := consumer.ConsumeTopic([]string{"192.168.1.11:9092", "192.168.1.11:19092", "192.168.1.11:29092"}, []string{"first"}, "first-test-1748858392")
	if err != nil {
		slog.Error("Consumer setup fail,", "err:", err)
	}
}
