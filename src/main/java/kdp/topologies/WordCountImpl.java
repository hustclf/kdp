package kdp.topologies;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;


/**
 * @author chenlifei
 */
public class WordCountImpl extends AbstractTopology implements Topology {

    public WordCountImpl(String topologyName) {
        super(topologyName);
        streamProperties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProperties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }

    @Override
    public org.apache.kafka.streams.Topology getTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(inputTopic);
        input.flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                .groupBy((key, value) -> value)
                .count()
                .toStream()
                .to(outputTopic);

        return builder.build();
    }
}
