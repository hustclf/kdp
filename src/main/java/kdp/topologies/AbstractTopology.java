package kdp.topologies;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * @author chenlifei
 */
public abstract class AbstractTopology implements Topology {

    final Logger logger = LoggerFactory.getLogger(this.getClass());
    String topologyName;
    String inputTopic;
    String outputTopic;
    Properties streamProperties;

    AbstractTopology(String topologyName) {
        this.topologyName = topologyName;

        setInputTopic();
        setOutputTopic();
        setStreamConfiguration();
    }

    void setInputTopic() {
        inputTopic = Optional.ofNullable(System.getenv("TOPIC_INPUT")).orElse("default-input");
    }

    @Override
    public String getInputTopic() {
        return inputTopic;
    }

    void setOutputTopic() {
        outputTopic = Optional.ofNullable(System.getenv("TOPIC_OUTPUT")).orElse(topologyName + "-output");
    }

    @Override
    public String getOutputTopic() {
        return outputTopic;
    }

    String getTopologyName() {
        return topologyName;
    }

    void setStreamConfiguration() {
        Properties streamsConfiguration = new Properties();

        String appId = Optional.ofNullable(System.getenv("APP_ID")).orElse(topologyName + "-output");
        String bootstrapServers = kdp.conf.Config.KAFKA_BROKER;

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, Optional.ofNullable(System.getenv("AUTO_OFFSET_RESET_CONFIG")).orElse("latest"));
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), Optional.ofNullable(System.getenv("ACKS_CONFIG")).orElse("all"));
        streamsConfiguration.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Integer.parseInt(Optional.ofNullable((System.getenv("REPLICATION_FACTOR_CONFIG"))).orElse("1")));
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.parseInt(Optional.ofNullable((System.getenv("NUM_STREAM_THREADS_CONFIG"))).orElse("1")));
        streamsConfiguration.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, Long.parseLong(Optional.ofNullable((System.getenv("WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG"))).orElse("86400000")));
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, Optional.ofNullable(System.getenv("PROCESSING_GUARANTEE_CONFIG")).orElse(StreamsConfig.AT_LEAST_ONCE));

        streamProperties = streamsConfiguration;
    }

    @Override
    public Properties getStreamProperties() {
        return streamProperties;
    }

    public void createTopics(List<String> topics) {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kdp.conf.Config.KAFKA_BROKER);
        AdminClient admin = AdminClient.create(config);

        List<NewTopic> newTopics = new ArrayList<>();

        for (String topicName : topics) {
            newTopics.add(new NewTopic(topicName, kdp.conf.Config.PARTITIONS, kdp.conf.Config.REPLICATION_FACTORS));
        }

        admin.createTopics(newTopics);
    }
}
