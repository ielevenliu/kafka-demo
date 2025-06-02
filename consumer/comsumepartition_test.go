package consumer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConsumePartition(t *testing.T) {
	err := ConsumePartition([]string{"192.168.1.11:9092"}, "first", 1)
	assert.Equal(t, nil, err)
}
