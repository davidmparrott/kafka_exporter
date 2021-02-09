package exporter

import (
	"github.com/Shopify/sarama"
	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	ioprometheusclient "github.com/prometheus/client_model/go"
	"regexp"
	"sync"
	"testing"
	"time"
)

func NewTestConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.MinVersion
	config.ClientID = "test_exporter"
	config.Metadata.RefreshFrequency, _ = time.ParseDuration("1m")
	return config
}

func NewKafkaOpts() Options {
	return Options{
		Uri:                  []string{"localhost:9092"},
		KafkaVersion:         sarama.V2_0_0_0.String(),
		MaxOffsets:           10,
		PruneIntervalSeconds: 10,
	}
}

var testMessage = sarama.StringEncoder("foo")
var expectedValues = map[string]float64{
	"kafka_topic_partitions":                           1,
	"kafka_topic_partition_leader":                     2,
	"kafka_topic_partition_current_offset":             456,
	"kafka_topic_partition_oldest_offset":              123,
	"kafka_topic_partition_replicas":                   0,
	"kafka_topic_partition_in_sync_replica":            0,
	"kafka_topic_partition_leader_is_preferred":        0,
	"kafka_topic_partition_under_replicated_partition": 0,
}

func TestMetricsForTopic(t *testing.T) {

	fakeBroker := sarama.NewMockBroker(t, 2)
	fakeBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(fakeBroker.Addr(), fakeBroker.BrokerID()).
			SetLeader("test_topic", 0, fakeBroker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("test_topic", 0, sarama.OffsetNewest, 456).
			SetOffset("test_topic", 0, sarama.OffsetOldest, 123),
		"FetchRequest": sarama.NewMockFetchResponse(t, 1).
			SetMessage("test_topic", 0, 9, testMessage).
			SetMessage("test_topic", 0, 10, testMessage).
			SetMessage("test_topic", 0, 11, testMessage),
	})

	config := NewTestConfig()
	config.Metadata.Retry.Max = 0
	client, err := sarama.NewClient([]string{fakeBroker.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}

	e := &Exporter{
		client:                  client,
		topicFilter:             regexp.MustCompile("test_topic"),
		groupFilter:             regexp.MustCompile("test_group"),
		mu:                      sync.Mutex{},
		useZooKeeperLag:         false,
		zookeeperClient:         nil,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: config.Metadata.RefreshFrequency,
		allowConcurrent:         true,
		sgMutex:                 sync.Mutex{},
		sgWaitCh:                nil,
		sgChans:                 []chan<- prometheus.Metric{},
		consumerGroupFetchAll:   true,
		consumerGroupLagTable:   interpolationMap{mu: sync.Mutex{}},
		kafkaOpts:               NewKafkaOpts(),
		saramaConfig:            config,
		logger:                  log.NewNopLogger(),
	}
	e.initializeMetrics()
	ch := make(chan prometheus.Metric)
	done := make(chan bool)
	go func() {
		testMetric := &ioprometheusclient.Metric{}
		for {
			channelMetric, more := <-ch
			if more {
				_ = channelMetric.Write(testMetric)
				metricString := channelMetric.Desc().String()

				if metricString == topicPartitions.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_topic_partitions"] {
						t.Errorf("incorrect number of partitions. Expected: %f Got: %f", expectedValues["kafka_topic_partitions"], *testMetric.Gauge.Value)
					}
				} else if metricString == topicPartitionLeader.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_topic_partition_leader"] {
						t.Errorf("incorrect leader id. Expected %f Got: %f", expectedValues["kafka_topic_partition_leader"], *testMetric.Gauge.Value)
					}
				} else if metricString == topicCurrentOffset.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_topic_partition_current_offset"] {
						t.Errorf("incorrect current offset. Expected %f Got %f", expectedValues["kafka_topic_partition_current_offset"], *testMetric.Gauge.Value)
					}
				} else if metricString == topicOldestOffset.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_topic_partition_oldest_offset"] {
						t.Errorf("incorrect oldest offset. Expected %f Got %f", expectedValues["kafka_topic_partition_oldest_offset"], *testMetric.Gauge.Value)
					}

				} else if metricString == topicPartitionReplicas.String() {

				} else if metricString == topicPartitionInSyncReplicas.String() {

				} else if metricString == topicPartitionUsesPreferredReplica.String() {

				} else if metricString == topicUnderReplicatedPartition.String() {

				} else {
					// TODO: unknown metric name
				}
			} else {
				done <- true
				return
			}
		}
	}()
	e.metricsForTopic("test_topic", ch)
	close(ch)
	<-done
}
