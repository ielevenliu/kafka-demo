package consumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ConsumeHandler struct{}

func (c *ConsumeHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumeHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumeHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				slog.Error("Consumer group is closed")
				return nil
			}
			slog.Info(fmt.Sprintf("Msg claim: topic=%s, partition=%d, offset=%d, timestamp=%+v, key=%s, value=%s",
				msg.Topic, msg.Partition, msg.Offset, msg.Timestamp, string(msg.Key), string(msg.Value)))
			session.MarkMessage(msg, "")
		case <-session.Context().Done():
			slog.Info("Consumer group session is closed")
			return nil
		}
	}
}

// 按 Topic 消费
func ConsumeTopic(addr, topics []string, groupId string) error {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_7_1_0
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Offsets.AutoCommit.Enable = true
	cfg.Consumer.Offsets.AutoCommit.Interval = time.Second
	// 消费者分组分配策略 & Rebalance策略
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRange()}

	cg, err := sarama.NewConsumerGroup(addr, groupId, cfg)
	if err != nil {
		slog.Error("Init consumer group fail,", "err:", err)
		return err
	}
	defer func() {
		if err = cg.Close(); err != nil {
			slog.Error("Close consumer group fail,", "err:", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err = cg.Consume(ctx, topics, &ConsumeHandler{}); err != nil {
				if errors.Is(err, sarama.ErrClosedConsumerGroup) {
					slog.Info("Close consumer group fail,", "err:", err)
					return
				}
				slog.Error("Start consumer group fail,", "err:", err)
				return
			}
			slog.Info("Consumer group closed...")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
		for {
			sig := <-signals
			if syscall.SIGINT == sig {
				slog.Info("SIGINT...")
				break
			}
			if syscall.SIGTERM == sig {
				slog.Info("SIGTERM...")
				break
			}
		}
		cancel()
	}()

	wg.Wait()
	return nil
}
