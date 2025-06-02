package consumer

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestConsumeTopic(t *testing.T) {
	err := ConsumeTopic([]string{"192.168.1.11:9092"}, []string{"first"}, "first-test-1748858392")
	assert.Equal(t, nil, err)
}

func TestConsumeTopic1(t *testing.T) {
	wg := sync.WaitGroup{}
	wg.Add(3)
	for i := 0; i < 3; i++ {
		go func() {
			defer wg.Done()
			err := ConsumeTopic([]string{"192.168.1.11:9092"}, []string{"first"}, "first-test-1748858392")
			assert.Equal(t, nil, err)
		}()
	}
	wg.Wait()
}
