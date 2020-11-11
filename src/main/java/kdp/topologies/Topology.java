package kdp.topologies;

import java.util.Properties;


/**
 * @author chenlifei
 */
public interface Topology {

    /**
     * get topology of kafka stream
     *
     * @return org.apache.kafka.streams.Topology topology
     */
    org.apache.kafka.streams.Topology getTopology();

    /**
     * get kafka stream configurations
     *
     * @return properties: configuration of kafka streams
     */
    Properties getStreamProperties();

    /**
     * get input topic
     *
     * @return input topic name
     */
    String getInputTopic();

    /**
     * get output topic
     *
     * @return output topic name
     */
    String getOutputTopic();
}
