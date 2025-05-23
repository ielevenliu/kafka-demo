package producer

import (
	"fmt"
	"github.com/IBM/sarama"
	"log/slog"
	"time"
)

func SyncProducer(addr []string, topic, data string) error {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForLocal // 只等待Leader节点落盘成功
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	// producer刷新策略
	cfg.Producer.Flush.Bytes = 16 * 1024                  // 当累积的消息大小达到16KB时发送 batch.size
	cfg.Producer.Flush.Messages = 100                     // 当累积的消息数量达到100条时发送
	cfg.Producer.Flush.Frequency = 100 * time.Millisecond // 每100毫秒尝试发送一次消息 linger.ms

	cli, err := sarama.NewSyncProducer(addr, cfg)
	if err != nil {
		slog.Error("Init sync producer fail,", "err:", err)
		return err
	}
	slog.Info("Init sync producer success")
	defer cli.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(data),
	}
	partition, offset, err := cli.SendMessage(msg)
	if err != nil {
		slog.Error("Send msg fail,", "err:", err)
		return err
	}
	slog.Info(fmt.Sprintf("partition: %d, offset: %d", partition, offset))
	return nil
}
