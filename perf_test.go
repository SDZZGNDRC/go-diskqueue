package diskqueue_test

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/go-diskqueue"
)

const (
	testDataPath  = "test_data"
	testQueueName = "test_queue"
	fileSize      = 1024 * 1024 // 1MB per file
	syncEvery     = 2500        // sync after this many writes
	syncTimeout   = 2 * time.Second
	numProducers  = 4                // 并发生产者数量
	numConsumers  = 4                // 并发消费者数量
	numMessages   = 100000           // 每个生产者发送的消息数
	messageSize   = 1024             // 每条消息大小
	testDuration  = 30 * time.Second // 测试持续时间
)

func TestDiskQueuePressure(t *testing.T) {
	defer os.RemoveAll(testDataPath)

	err := os.Mkdir(testDataPath, os.ModePerm)
	if err != nil {
		t.Fatal(err.Error())
	}

	// 初始化 diskqueue
	dq := diskqueue.New(
		testQueueName,
		testDataPath,
		fileSize,
		int32(messageSize),   // minMsgSize
		int32(messageSize*2), // maxMsgSize
		syncEvery,
		syncTimeout,
		func(lvl diskqueue.LogLevel, f string, args ...interface{}) {
			// t.Logf(f, args...)
		},
	)
	defer dq.Close()

	var (
		producerWG sync.WaitGroup
		consumerWG sync.WaitGroup
		totalSent  uint64
		totalRecv  uint64
		startTime  = time.Now()
		stopCh     = make(chan struct{})
	)

	// 启动生产者
	for i := 0; i < numProducers; i++ {
		producerWG.Add(1)
		go func(producerID int) {
			defer producerWG.Done()

			data := bytes.Repeat([]byte{byte(producerID)}, messageSize)
			for seq := 0; ; seq++ {
				select {
				case <-stopCh:
					return
				default:
					msg := generateMessage(producerID, seq, data)
					if err := dq.Put(msg); err != nil {
						t.Errorf("Producer %d error: %v", producerID, err)
						return
					}
					atomic.AddUint64(&totalSent, 1)
					if totalSent >= numMessages*uint64(numProducers) {
						return
					}
				}
			}
		}(i)
	}

	// 启动消费者
	for i := 0; i < numConsumers; i++ {
		consumerWG.Add(1)
		go func(consumerID int) {
			defer consumerWG.Done()

			for {
				select {
				case <-stopCh:
					return
				case data := <-dq.ReadChan():
					atomic.AddUint64(&totalRecv, 1)
					if !validateMessage(data) {
						t.Errorf("Consumer %d received invalid message", consumerID)
					}
				case <-time.After(100 * time.Millisecond):
					// 防止阻塞太久
				}
			}
		}(i)
	}

	// 定时停止测试
	go func() {
		<-time.After(testDuration)
		close(stopCh)
	}()

	// 等待测试完成
	producerWG.Wait()
	t.Logf("Producers finished. Total sent: %d", atomic.LoadUint64(&totalSent))

	// 等待消费者处理剩余消息
	time.Sleep(2 * time.Second)
	close(stopCh)
	consumerWG.Wait()

	// 输出统计结果
	duration := time.Since(startTime).Seconds()
	t.Logf("Test completed in %.2f seconds", duration)
	t.Logf("Total sent: %d (%.2f msg/sec)",
		totalSent, float64(totalSent)/duration)
	t.Logf("Total received: %d (%.2f msg/sec)",
		totalRecv, float64(totalRecv)/duration)
	t.Logf("Queue depth: %d", dq.Depth())

	bytesSent := atomic.LoadUint64(&totalSent) * uint64(messageSize)
	mbPerSec := float64(bytesSent) / 1024 / 1024 / duration
	t.Logf("Write throughput: %.2f MB/s", mbPerSec)

	// 验证数据完整性
	if totalSent != totalRecv {
		t.Errorf("Data loss occurred! Sent: %d, Received: %d", totalSent, totalRecv)
	}
}

// 生成包含生产者ID和序列号的可验证消息
func generateMessage(producerID, seq int, data []byte) []byte {
	header := fmt.Sprintf("[producer:%d seq:%d]", producerID, seq)
	return append([]byte(header), data...)
}

// 验证消息格式和内容
func validateMessage(data []byte) bool {
	if len(data) < 20 { // 简单验证最小长度
		return false
	}
	// 这里可以添加更详细的验证逻辑
	return true
}

// 清理测试数据
func TestMain(m *testing.M) {
	os.RemoveAll(testDataPath)
	code := m.Run()
	os.RemoveAll(testDataPath)
	os.Exit(code)
}
