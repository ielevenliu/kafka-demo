package consumer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestConsumeTopic(t *testing.T) {
	err := ConsumeTopic([]string{"192.168.1.11:9092"}, []string{"first"}, "first-test-1748858392")
	assert.Equal(t, nil, err)
}
