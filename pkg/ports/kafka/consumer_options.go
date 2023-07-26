package kafkaconsumer

import (
	"time"

	"github.com/segmentio/kafka-go"
)

type Option = func(s *kafka.ReaderConfig) error

// WithGroupID sets the consumer group id.
// By default, the group id is set to the empty string,
// which disables consumer groups.
func WithGroupID(groupID string) Option {
	return func(s *kafka.ReaderConfig) error {
		s.GroupID = groupID
		return nil
	}
}

// WithPartition sets the partition to consume.
// By default, the partition is set to -1,
// which means that the reader consumes from all partitions.
func WithPartition(partition int) Option {
	return func(s *kafka.ReaderConfig) error {
		s.Partition = partition
		return nil
	}
}

// WithMaxWait sets the maximum amount of time to wait
// for new data to come when fetching batches of messages from kafka.
func WithMaxWait(m int) Option {
	return func(s *kafka.ReaderConfig) error {
		s.MaxWait = time.Duration(m) * time.Second
		return nil
	}

}

func WithMinBytes(m int) Option {
	return func(s *kafka.ReaderConfig) error {
		s.MinBytes = m
		return nil
	}
}

func WithMaxBytes(m int) Option {
	return func(s *kafka.ReaderConfig) error {
		s.MaxBytes = m
		return nil
	}
}

func WithOffset(offset int64) Option {
	return func(s *kafka.ReaderConfig) error {
		s.StartOffset = offset
		return nil
	}
}
