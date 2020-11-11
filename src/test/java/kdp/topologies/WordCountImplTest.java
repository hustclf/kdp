package kdp.topologies;

import kdp.TestTopology;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class WordCountImplTest extends TestTopology {
    @Override
    @Before
    public void setup() {}

    /**
     * Produce some input data to the input topic.
     */
    @Override
    protected void producer() throws Exception {
        for (int i = 0; i < inputLogs.length(); i++) {
            JSONObject log = inputLogs.getJSONObject(i);
            testDriver.pipeInput(recordFactory.create(topology.getInputTopic(), log.getString("key"), log.getString("value"), 9999L));
        }
    }

    @Override
    protected void runTopology() throws Exception {
        String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
        JSONObject currentTestConf = conf.getJSONObject(methodName);

        inputLogs = currentTestConf.getJSONArray("inputLogs");
        expectOutputLogs = currentTestConf.getJSONArray("outputLogs");

        producer();
    }

    @Test
    public void run() throws Exception {
        super.setup();
        runTopology();
        verify();
    }

    @Override
    protected void verify() throws Exception {
        for (int i = 0; i < expectOutputLogs.length(); i++) {
            JSONObject log = expectOutputLogs.getJSONObject(i);
            String expectKey = log.getString("key");
            Long expectValue = log.getLong("value");

            ProducerRecord<String, Long> actualLog = testDriver.readOutput(topology.getOutputTopic(), stringDeserializer, longDeserializer);
            assertThat(actualLog.key()).isEqualTo(expectKey);
            assertThat(actualLog.value()).isEqualTo(expectValue);
        }

        Assert.assertNull(testDriver.readOutput(topology.getOutputTopic(), stringDeserializer, longDeserializer));
    }
}
