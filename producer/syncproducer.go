package producer

import (
	"fmt"
	"github.com/IBM/sarama"
	"log/slog"
)

func SyncProducer(addr []string, topic, data string) error {
	cfg := sarama.NewConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForLocal
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

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
