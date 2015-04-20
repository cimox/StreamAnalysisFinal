package edu.cimo.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.cimo.util.Timestamp;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.Map;

/**
 * Created by cimo on 18/04/15.
 */
public class HashtagExtract implements IRichBolt {
    private OutputCollector _collector;
    private Long _cnt;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        _cnt = new Long(0);
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            JSONObject tweet = (JSONObject) tuple.getValueByField("tweet");
            JSONArray hashtags = tweet.getJSONArray("hashtagEntities");
            Timestamp.markWithTimestamp(tweet, "timestamp-hashtag-extract");

            // Iterate over all hashtags end emit
            for (int i = 0; i < hashtags.length(); i++) {
                String tag = hashtags.getJSONObject(i).getString("text");
                _collector.emit(new Values(tag, tweet));
            }
            if (hashtags.length() == 0) {
                // Nothing to emit, or emit as a NULL value
//                System.out.println("[WARN] No hashtags, nothing to emit.");

                // Emit hashtags as a NULL value
//                System.out.println("[WARN] emitting WITH NO hashtags!!!");
                _collector.emit(new Values(null, tweet));
            }
        } catch (JSONException e) {
            System.err.println("[ERROR] in thread " + Thread.currentThread() + ": " + e.getMessage());
        }

        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("hashtag", "tweet"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
