package kafkaconsumer

import (
	"context"
	"errors"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"strings"
	"time"
)

var (
	ErrConsumerAlreadyStarted = errors.New("consumer already started")
)

type Consumer struct {
	reader *kafka.Reader
	errors   chan error
}

func NewConsumer(brokerUrls, topic string, opts ...Option) *Consumer {
	c := Consumer{}

	config := kafka.ReaderConfig{
		Brokers:  strings.Split(brokerUrls, ","),
		Topic:    topic,
		MinBytes: 10e3, // 10KB
		MaxBytes: 10e6, // 10MB
		MaxWait:  10 * time.Second,
	}

	for _, opt := range opts {
		err := opt(&config)
		if err != nil {
			panic(err)
		}
	}

	c.reader = kafka.NewReader(config)
	c.errors = make(chan error,1)

	return &c
}


func (c *Consumer) Listen(ctx context.Context) { // Use context passed from outside
    defer c.Close()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Context done...")
			return 
		default:
			log.Info().Msg("Listening...")
			m, err := c.reader.ReadMessage(ctx)
			if err != nil {
				if errors.Is(err, context.Canceled) {
					// Context was canceled, this is not considered an error in our case.
					log.Info().Msg("Context was canceled, stopping the loop...")
					return 
				}
				// It's another kind of error.
				log.Error().Err(err).Msg("Error on reading message")
				c.errors <- err
				continue 
			}

			timestamp := m.Time.UTC().Format(time.RFC3339)
			log.Info().Msgf("message at offset %d: %s\n", m.Offset, timestamp)
			log.Info().Msgf("message at offset %d: %s = %s\n", m.Offset,
				string(m.Key), string(m.Value))
		}
	}

}

func (c *Consumer) Errors() chan error {
    return c.errors
}

func (c *Consumer) Close() {
	log.Info().Msg("Closing reader...")
	c.reader.Close()
	log.Info().Msg("Closing reader...done")
	return 
}