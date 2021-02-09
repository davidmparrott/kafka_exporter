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
	config.Version = sarama.MaxVersion
	config.ClientID = "test_exporter"
	config.Metadata.RefreshFrequency, _ = time.ParseDuration("1m")
	return config
}

var testMessage = sarama.StringEncoder("foo")

func TestMetricsForTopic(t *testing.T) {
	//Setup mocks
	fakeBroker := sarama.NewMockBroker(t, 2)
	fakeBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(fakeBroker.Addr(), fakeBroker.BrokerID()).
			SetLeader("test_topic", 0, fakeBroker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetOffset("test_topic", 0, sarama.OffsetNewest, 456).
			SetOffset("test_topic", 0, sarama.OffsetOldest, 123),
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
		useZooKeeperLag:         false, // TODO: find a way to mock/fake kazoo to test these metrics
		zookeeperClient:         nil,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: config.Metadata.RefreshFrequency,
		allowConcurrent:         true,
		sgMutex:                 sync.Mutex{},
		sgWaitCh:                nil,
		sgChans:                 []chan<- prometheus.Metric{},
		consumerGroupFetchAll:   true,
		consumerGroupLagTable:   interpolationMap{mu: sync.Mutex{}},
		kafkaOpts:               Options{},
		saramaConfig:            config,
		logger:                  log.NewNopLogger(),
	}
	e.initializeMetrics()

	//Table of expected values for metrics
	expectedValues := map[string]float64{
		"kafka_topic_partitions":                           1,
		"kafka_topic_partition_leader":                     2,
		"kafka_topic_partition_current_offset":             456,
		"kafka_topic_partition_oldest_offset":              123,
		"kafka_topic_partition_replicas":                   1,
		"kafka_topic_partition_in_sync_replica":            1,
		"kafka_topic_partition_leader_is_preferred":        1,
		"kafka_topic_partition_under_replicated_partition": 0,
	}

	//Read from metric output channel until closed
	//Check whether expected gauge value for metric matches expected value in table
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
					if *testMetric.Gauge.Value != expectedValues["kafka_topic_partition_replicas"] {
						t.Errorf("incorrect count of partition replicas. Expected %f Got %f", expectedValues["kafka_topic_partition_replicas"], *testMetric.Gauge.Value)
					}
				} else if metricString == topicPartitionInSyncReplicas.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_topic_partition_in_sync_replica"] {
						t.Errorf("incorrect count of in-sync replicas. Expected %f Got %f", expectedValues["kafka_topic_partition_in_sync_replica"], *testMetric.Gauge.Value)
					}
				} else if metricString == topicPartitionUsesPreferredReplica.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_topic_partition_leader_is_preferred"] {
						t.Errorf("incorrect value for preferred broker. Expected %f Got %f", expectedValues["kafka_topic_partition_leader_is_preferred"], *testMetric.Gauge.Value)
					}
				} else if metricString == topicUnderReplicatedPartition.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_topic_partition_under_replicated_partition"] {
						t.Errorf("incorrect value for under-replication. Expected %f Got %f", expectedValues["kafka_topic_partition_under_replicated_partition"], *testMetric.Gauge.Value)
					}
				} else {
					t.Errorf("unexpected metric received: %s", metricString)
				}
			} else {
				done <- true
				return
			}
		}
	}()
	e.metricsForTopic(ch)
	close(ch)
	<-done
}

func TestMetricsForConsumerGroup(t *testing.T) {
	//Setup mocks
	fakeBroker := sarama.NewMockBroker(t, 2)
	fakeBroker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(fakeBroker.Addr(), fakeBroker.BrokerID()).
			SetLeader("test_topic", 0, fakeBroker.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(t).
			SetVersion(2). //must set API version to avoid https://github.com/Shopify/sarama/issues/938
			SetOffset("test_topic", 0, sarama.OffsetNewest, 456),
		"ListGroupsRequest": sarama.NewMockListGroupsResponse(t).
			AddGroup("test_group", "consumer"),
		"DescribeGroupsRequest": sarama.NewMockDescribeGroupsResponse(t).
			AddGroupDescription("test_group", &sarama.GroupDescription{
				GroupId: "test_group",
				Members: map[string]*sarama.GroupMemberDescription{"test": {}},
			}),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(t).
			SetOffset("test_group", "test_topic", 0, 10, "", sarama.KError(0)),
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
		useZooKeeperLag:         false, // TODO: find a way to mock/fake kazoo to test these metrics
		zookeeperClient:         nil,
		nextMetadataRefresh:     time.Now(),
		metadataRefreshInterval: config.Metadata.RefreshFrequency,
		allowConcurrent:         true,
		sgMutex:                 sync.Mutex{},
		sgWaitCh:                nil,
		sgChans:                 []chan<- prometheus.Metric{},
		consumerGroupFetchAll:   true,
		consumerGroupLagTable:   interpolationMap{mu: sync.Mutex{}},
		kafkaOpts:               Options{},
		saramaConfig:            config,
		logger:                  log.NewNopLogger(),
	}
	e.initializeMetrics()

	expectedValues := map[string]float64{
		"kafka_consumergroup_members":                 1,
		"kafka_consumergroup_current_offset":          10,
		"kafka_consumergroup_uncommitted_offsets":     446,
		"kafka_consumergroup_current_offset_sum":      10,
		"kafka_consumergroup_uncommitted_offsets_sum": 446,
	}

	//Read from metric output channel until closed
	//Check whether expected gauge value for metric matches expected value in table
	ch := make(chan prometheus.Metric)
	done := make(chan bool)
	go func() {
		testMetric := &ioprometheusclient.Metric{}
		for {
			channelMetric, more := <-ch
			if more {
				_ = channelMetric.Write(testMetric)
				metricString := channelMetric.Desc().String()
				if metricString == consumergroupMembers.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_consumergroup_members"] {
						t.Errorf("unexpected number of consumergroup members. Expected: %f Got: %f", expectedValues["kafka_consumergroup_members"], *testMetric.Gauge.Value)
					}
				} else if metricString == consumergroupCurrentOffset.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_consumergroup_current_offset"] {
						t.Errorf("unexpected current offset. Expected: %f Got: %f", expectedValues["kafka_consumergroup_current_offset"], *testMetric.Gauge.Value)
					}
				} else if metricString == consumergroupUncommittedOffsets.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_consumergroup_uncommitted_offsets"] {
						t.Errorf("unexpected value for uncommitted offsets. Expected: %f Got: %f", expectedValues["kafka_consumergroup_uncommitted_offsets"], *testMetric.Gauge.Value)
					}
				} else if metricString == consumergroupCurrentOffsetSum.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_consumergroup_current_offset_sum"] {
						t.Errorf("unexpected current offset sum. Expected: %f Got: %f", expectedValues["kafka_consumergroup_current_offset_sum"], *testMetric.Gauge.Value)
					}
				} else if metricString == consumergroupUncommittedOffsetsSum.String() {
					if *testMetric.Gauge.Value != expectedValues["kafka_consumergroup_uncommitted_offsets_sum"] {
						t.Errorf("unexpected uncommitted offsets sum. Expected: %f Got: %f", expectedValues["kafka_consumergroup_uncommitted_offsets_sum"], *testMetric.Gauge.Value)
					}
				}
			} else {
				done <- true
				return
			}
		}
	}()
	e.metricsForConsumerGroup(ch)
	close(ch)
	<-done
}
