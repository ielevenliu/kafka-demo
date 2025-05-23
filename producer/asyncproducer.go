package producer

import (
	"fmt"
	"github.com/IBM/sarama"
	"log/slog"
	"sync"
	"time"
)

func AsyncProducer(addr []string, topic, data string) error {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForLocal       // 只等待Leader节点落盘成功
	cfg.Producer.Return.Successes = true                  // 返回成功信息
	cfg.Producer.Return.Errors = true                     // 返回错误信息
	cfg.Producer.Flush.Bytes = 16 * 1024                  // 当累积的消息大小达到16KB时发送 batch.size
	cfg.Producer.Flush.Messages = 100                     // 当累积的消息数量达到100条时发送
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond // 每100毫秒尝试发送一次消息 linger.ms
	//cfg.Producer.Partitioner = sarama.NewManualPartitioner // 指定手动分区器，在消息体中配置的Partition才会生效

	producer, err := sarama.NewAsyncProducer(addr, cfg)
	if err != nil {
		slog.Error("Init async producer fail,", "err:", err)
		return err
	}
	slog.Info("Init async producer success")

	wg := sync.WaitGroup{}
	wg.Add(2)

	// 遍历成功消息
	go func() {
		defer wg.Done()
		for success := range producer.Successes() {
			slog.Info(fmt.Sprintf("partition: %d, offset: %d", success.Partition, success.Offset))
		}
		slog.Info("Successes channel closed")
	}()

	// 遍历失败消息
	go func() {
		defer wg.Done()
		for fail := range producer.Errors() {
			slog.Error("Send msg fail,", "err:", fail.Err)
		}
		slog.Info("Errors channel closed")
	}()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		//Partition: int32(1), // 指定分区，配合手动分区器使用
		//Key: sarama.StringEncoder(topic), // 指定key，分区器会对key进行hash获取分区id
	}
	for i := 0; i < 10; i++ {
		msg.Value = sarama.StringEncoder(fmt.Sprintf("%s-%d", data, i))
		producer.Input() <- msg
		time.Sleep(time.Second)
	}
	if err = producer.Close(); err != nil {
		slog.Error("Close async producer fail,", "err:", err)
	}
	wg.Wait()

	return nil
}
