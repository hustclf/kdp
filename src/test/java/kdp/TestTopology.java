package kdp;

import kdp.utils.Functions;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;


public class TestTopology {

    private final static Logger logger = LoggerFactory.getLogger(TestTopology.class);
    protected static TopologyTestDriver testDriver;
    @Rule
    public final EnvironmentVariables environmentVariables
            = new EnvironmentVariables();
    protected StringDeserializer stringDeserializer = new StringDeserializer();
    protected LongDeserializer longDeserializer = new LongDeserializer();
    protected ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(new StringSerializer(), new StringSerializer());
    protected kdp.topologies.Topology topology;
    protected JSONObject conf = new JSONObject();
    protected JSONArray inputLogs;
    protected JSONArray expectOutputLogs;

    public void setup(String testDataFolder) {
        String className = this.getClass().getSimpleName();
        String topologyName = Optional.ofNullable(System.getenv("CURRENT_TOPOLOGY"))
                .orElse(className.replace("Test", ""));

        topology = TopologyFactory.getTopology(topologyName);

        logger.info(topology.getTopology().describe().toString());

        testDriver = new TopologyTestDriver(topology.getTopology(), topology.getStreamProperties());
        try {
            conf = new JSONObject(Functions.readJsonFile(String.format("unit_test/%s/%sTest", testDataFolder, topologyName)));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    @Before
    public void setup() {
        this.setup("topologies");
    }

    protected void runTopology() throws Exception {
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        JSONObject currentTestConf = conf.getJSONObject(methodName);

        inputLogs = currentTestConf.getJSONArray("inputLogs");
        expectOutputLogs = currentTestConf.getJSONArray("outputLogs");

        producer();
    }

    /**
     * Produce some input data to the input topic.
     */
    protected void producer() throws Exception {
        for (int i = 0; i < inputLogs.length(); i++) {
            JSONObject log = inputLogs.getJSONObject(i);
            testDriver.pipeInput(recordFactory.create(topology.getInputTopic(), log.getString("key"), log.getJSONObject("value").toString(), 9999L));
        }
    }

    /**
     * Verify the application's output data.
     */
    protected void verify() throws Exception {
        for (int i = 0; i < expectOutputLogs.length(); i++) {
            JSONObject log = expectOutputLogs.getJSONObject(i);
            String expectKey = log.getString("key");
            String expectValue = log.getJSONObject("value").toString();

            ProducerRecord<String, String> actualLog = testDriver.readOutput(topology.getOutputTopic(), stringDeserializer, stringDeserializer);
            assertThat(actualLog.key()).isEqualTo(expectKey);
            assertThat(actualLog.value()).isEqualTo(expectValue);
        }

        Assert.assertNull(testDriver.readOutput(topology.getOutputTopic(), stringDeserializer, longDeserializer));
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (NullPointerException e) {
            e.printStackTrace();
        }
    }

}
