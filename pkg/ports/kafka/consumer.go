package kafkaconsumer

import (
	"context"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"github.com/segmentio/kafka-go"
	"strings"
	"time"
)

var (
	ErrConsumerAlreadyStarted = errors.New("consumer already started")
)

type Consumer struct {
	errCh  chan error
	reader *kafka.Reader
}

func NewConsumer(brokerUrls, topic string, opts ...Option) *Consumer {
	c := Consumer{}
	//c.errCh = make(chan error)

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

	return &c
}

//* TODO LISTEN GA DI BREAK

// func (c *Consumer) Listen() error {
// 	ctx := context.Background()

// 	// while loop so always listen to channel
// 	// function needs to continuously consume messages until the context is cancelled or an error occurs,
// 	go func() {
// 		for {

// 			fmt.Println("LISTENING")
// 			m, err := c.reader.ReadMessage(ctx)
// 			if err != nil {
// 				fmt.Println(err, "ERROR GAN")
// 				c.errCh <- err
// 				return
// 			}

// 			timestamp := m.Time.UTC().Format(time.RFC3339)

// 			log.Info().Msgf("message at offset %d: %s\n", m.Offset, timestamp)
// 			log.Info().Msgf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
// 		}
// 	}()

// 	return nil
// }

func (c *Consumer) Listen() error { // Use context passed from outside
	ctx := context.Background()

	for {
		log.Info().Msg("Listening...")
		m, err := c.reader.ReadMessage(ctx)
		if err != nil {
			fmt.Println(err, "ERROR GAN")
			if errors.Is(err, context.Canceled) {
				// Context was canceled, this is not considered an error in our case.
				log.Info().Msg("Context was canceled, stopping the loop...")
				c.reader.Close()
				return nil
			}

			// It's another kind of error.
			log.Error().Err(err).Msg("Error on reading message")
			//c.errCh <- err
			return nil
		}

		timestamp := m.Time.UTC().Format(time.RFC3339)
		log.Info().Msgf("message at offset %d: %s\n", m.Offset, timestamp)
		log.Info().Msgf("message at offset %d: %s = %s\n", m.Offset,
			string(m.Key), string(m.Value))
	}

}

func (c *Consumer) Close() error {
	
	log.Info().Msg("Closing reader...")
	err := c.reader.Close()
	if err != nil {
		log.Error().Err(err).Msg("Failed to close reader")
	}
	return nil
}

func (c *Consumer) Error() chan error {
	return c.errCh
}
