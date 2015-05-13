package edu.cimo;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import edu.cimo.builder.TwitterTopologyBuilder;
import twitter4j.JSONObject;


/**
 * Created by cimo on 06/03/15.
 */
public class LocalTopologyRunner {
    private static final int TEN_SECONDS = 10000;
    private static final int ONE_MINUTE = 60000;


    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        // Create topology
        StormTopology topology = TwitterTopologyBuilder.build();

        // Config
        Config config = new Config();
        config.setFallBackOnJavaSerialization(false);
//        config.setNumWorkers(2);
//        config.setMessageTimeoutSecs(60);
//        config.registerSerialization(JSONObject.class);

        // Local cluster
        // I updated the "topology.spout.max.batch.size" value in config to about 64*1024 value and then storm processing became fast.
        LocalCluster cluster = new LocalCluster();

        // Submitting topology to local cluster and shutting down.
        cluster.submitTopology("twitter-live-stream-topology", config, topology);
        Utils.sleep(TEN_SECONDS * 21);
        cluster.killTopology("twitter-live-stream-topology");
        cluster.shutdown();

        // Remote cluster
//        StormSubmitter.submitTopologyWithProgressBar("production-deploy-test-2", config, topology);
    }
}
