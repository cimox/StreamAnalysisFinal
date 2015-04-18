package builder;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import spout.TwitterLiveSpout;
import bolt.PrintToFile;

/**
 * Created by cimo on 17/04/15.
 */
public class TwitterTopologyBuilder {
    public static StormTopology build() {
        TopologyBuilder builder = new TopologyBuilder();

        // Twitter live feed
        TwitterLiveSpout twitterLiveFeed = new TwitterLiveSpout("xkFpGc3EFeTP2FS9Igl7oP3wb", "fkV8QXHue64DU7m6MZUnfsG6hychQ6OxFjhK29jc89JTGnRf2E",
                "322790203-kNpf81448FSHgoM3nO1vb0PIobeTXuEUW8vAkX6Y", "TkO5gYaiBvKTVkFRbN5pc1dE0ADCl8GUdukIhnugYngjb", null);
        builder.setSpout("SPOUT_twitter-live-feed", twitterLiveFeed, 1);

        // Print to file, name is based on first HashTag
        builder.setBolt("BOLT_print-to-file", new PrintToFile(), 1)
                .shuffleGrouping("SPOUT_twitter-live-feed");

        return builder.createTopology();
    }
}
