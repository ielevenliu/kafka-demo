package producer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSampleProducer(t *testing.T) {
	err := SyncProducer([]string{"192.168.1.11:9092"}, "first", "hello")
	assert.Equal(t, nil, err)
}
