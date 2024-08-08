package kafka_service

import (
	"strings"

	_ "net/http/pprof"

	"github.com/IBM/sarama"
)

// Sarama configuration options

func Init(brokers string, topic string) *Kafka_helper {
	return &Kafka_helper{
		brokers:   brokers,
		version:   sarama.DefaultVersion.String(),
		topic:     topic,
		producers: 1,
		verbose:   false,
	}
}

type Kafka_helper struct {
	brokers   string
	version   string
	topic     string
	producers int //1
	verbose   bool
	oldest    bool // true
}

func (service *Kafka_helper) ProduceNewTransaction() {
	producer := newProducerProvider(strings.Split(service.brokers, ","), func() *sarama.Config {
		config := sarama.NewConfig()
		config.Producer.Idempotent = true
		config.Producer.Return.Errors = false
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		config.Producer.Transaction.Retry.Backoff = 10
		config.Producer.Transaction.ID = "txn_producer"
		config.Net.MaxOpenRequests = 1
		return config
	})

	producer.produceRecord(service.topic)
}

func (service *Kafka_helper) StartConsume(group string) {
	startConsume(group, service.verbose, service.oldest, service.version, service.brokers, service.topic)
}
