package producer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAsyncProducer(t *testing.T) {
	err := AsyncProducer([]string{"192.168.1.11:9092"}, "first", "hi")
	assert.Equal(t, nil, err)
}
