package kdp;

import kdp.conf.Config;
import kdp.topologies.Topology;
import org.apache.kafka.streams.KafkaStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;


/**
 * MainTopology: Entry for data-pipe application
 *
 * @author chenlifei
 */
public class MainTopology {

    final private static Logger logger = LoggerFactory.getLogger(MainTopology.class);

    private static String currentTopology = Config.CURRENT_TOPOLOGY;

    public static void main(final String[] args) {
        Topology topology = TopologyFactory.getTopology(currentTopology);
        final KafkaStreams streams = new KafkaStreams(topology.getTopology(), topology.getStreamProperties());

        logger.info(topology.getTopology().describe().toString());

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        // only works at local develop environments
        streams.cleanUp();

        streams.start();
        // catch uncaught runtime exception, close the stream and shutdown the instance
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            logger.error("Error Found, The Topology Will Exit", throwable);

            streams.close();
            latch.countDown();
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Latch interrupted", e);
            System.exit(1);
        }

        System.exit(0);
    }

}
