package consumer

import (
	"fmt"
	"github.com/IBM/sarama"
	"log/slog"
	"time"
)

// 按 Partition 消费
func ConsumePartition(addr []string, topic string, partition int32) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_1_0
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = time.Second

	c, err := sarama.NewConsumer(addr, cfg)
	if err != nil {
		slog.Error("Init consumer client fail,", "err:", err)
		return err
	}
	defer func() {
		if err = c.Close(); err != nil {
			slog.Error("Close consumer client fail,", "err:", err)
		}
	}()

	pc, err := c.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		slog.Error("Init partition consumer client fail,", "err:", err)
		return err
	}
	defer func() {
		if err = pc.Close(); err != nil {
			slog.Error("Close partition consumer client fail,", "err:", err)
		}
	}()

	for {
		select {
		case msg := <-pc.Messages():
			slog.Info(fmt.Sprintf("Msg: topic=%s, partition=%d, offset=%d, timestamp=%+v, key=%s, value=%s",
				msg.Topic, msg.Partition, msg.Offset, msg.Timestamp, msg.Key, msg.Value))
		case fail := <-pc.Errors():
			slog.Error("Consume msg fail,", "err:", fail.Error())
			return fail.Err
		}
	}
}
