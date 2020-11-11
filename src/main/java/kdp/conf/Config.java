package kdp.conf;

import java.util.Optional;


/**
 * Conf
 *
 * @author chenlifei
 */
public class Config {
    public static final String KAFKA_BROKER = Optional.ofNullable(System.getenv("KAFKA_BROKER"))
            .orElse("localhost:9092");

    public static final int PARTITIONS = Integer.parseInt(Optional.ofNullable(System.getenv("PARTITIONS"))
            .orElse("10"));

    public static final short REPLICATION_FACTORS = Short.parseShort(Optional.ofNullable(System.getenv("REPLICATION_FACTORS"))
            .orElse("3"));

    public static final String CURRENT_TOPOLOGY = Optional.ofNullable(System.getenv("CURRENT_TOPOLOGY"))
            .orElse("WordCountImpl");
}
