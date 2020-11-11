package kdp;

import kdp.topologies.Topology;

import java.lang.reflect.Constructor;

/**
 * a factory used for producing topologies
 *
 * @author chenlifei
 */
public class TopologyFactory {
    /**
     * get topology with certain topology name
     *
     * @param topologyName
     * @return topology
     */
    public static Topology getTopology(String topologyName) {
        Topology topology;
        try {
            Class<?> clazz = Class.forName("kdp.topologies." + topologyName);
            Constructor<?> constructor = clazz.getConstructor(String.class);
            topology = (Topology) constructor.newInstance(topologyName);
        } catch (Exception e) {
            throw new RuntimeException("Topology init error", e);
        }

        return topology;
    }

}