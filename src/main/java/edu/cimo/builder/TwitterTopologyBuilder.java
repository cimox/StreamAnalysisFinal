package edu.cimo.builder;

import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import edu.cimo.bolt.*;
import edu.cimo.scheme.KafkaCustomScheme;
import edu.cimo.spout.TwitterLiveSpout;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.UUID;

/**
 * Created by cimo on 17/04/15.
 */
public class TwitterTopologyBuilder {
    private static final short THREADS_NUM = 4;

    public static StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();

        /*
         * SPOUTS
         */
        // Twitter live feed
//        TwitterLiveSpout twitterLiveFeed = new TwitterLiveSpout("xkFpGc3EFeTP2FS9Igl7oP3wb", "fkV8QXHue64DU7m6MZUnfsG6hychQ6OxFjhK29jc89JTGnRf2E",
//                "322790203-kNpf81448FSHgoM3nO1vb0PIobeTXuEUW8vAkX6Y", "TkO5gYaiBvKTVkFRbN5pc1dE0ADCl8GUdukIhnugYngjb", null);
//        builder.setSpout("SPOUT_twitter-live-feed", twitterLiveFeed, 1);

//      Kafka spout which listen to kafka (at localhost) and emits messages to topology
        ZkHosts hosts = new ZkHosts("localhost:2181");
        SpoutConfig kafkaConfig = new SpoutConfig(hosts, "tweet", "/tweet", UUID.randomUUID().toString());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new KafkaCustomScheme("tweet"));
        KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        builder.setSpout("SPOUT_twitter-dataset-feed", kafkaSpout, (THREADS_NUM > 1) ? Math.floor(THREADS_NUM/2) : 1 );

        // Another Kafka spout emitting queries
        SpoutConfig kafkaConfigQuery = new SpoutConfig(hosts, "query", "/query", UUID.randomUUID().toString());
        kafkaConfigQuery.scheme = new SchemeAsMultiScheme(new KafkaCustomScheme("query"));
        KafkaSpout kafkaSpoutQuery = new KafkaSpout(kafkaConfigQuery);
        builder.setSpout("SPOUT_query-feed", kafkaSpoutQuery, 1);


        /*
         * BOLTS
         */
        // Get timestamp when tuple enter topology
        builder.setBolt("BOLT_enter-timestamp", new EnterTopologyTimestamp(), THREADS_NUM)
                .shuffleGrouping("SPOUT_twitter-dataset-feed");

        // At first, extract hashtags from tweets
        builder.setBolt("BOLT_hashtag-extract", new HashtagExtract(), THREADS_NUM)
                .shuffleGrouping("BOLT_enter-timestamp");

        // Filter tweets based on hashtag queries
        builder.setBolt("BOLT_hashtag-filter", new FilterByHashtag(), THREADS_NUM)
//                .shuffleGrouping("BOLT_hashtag-extract")
//                .allGrouping("SPOUT_query-feed");
                .fieldsGrouping("BOLT_hashtag-extract", new Fields("hashtag"))
                .fieldsGrouping("SPOUT_query-feed", new Fields("query"));

        // Extract only usable and data we wants
        builder.setBolt("BOLT_data-extract", new DataExtract(), THREADS_NUM)
                .shuffleGrouping("BOLT_hashtag-filter");

        // Finally, print filtered tweets to files.s
//        builder.setBolt("BOLT_print-to-file", new PrintToFile(), 4)
//                .fieldsGrouping("BOLT_data-extract", new Fields("hashtag"));

        // Finally, print filtered tweets to Redis
        builder.setBolt("BOLT_print-to-redis", new PrintToRedis(), THREADS_NUM)
                .fieldsGrouping("BOLT_data-extract", new Fields("hashtag"));

        return builder.createTopology();
    }
}
