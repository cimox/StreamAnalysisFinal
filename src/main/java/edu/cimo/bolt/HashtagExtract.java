package edu.cimo.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.Map;

/**
 * Created by cimo on 18/04/15.
 */
public class HashtagExtract implements IRichBolt {
    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        JSONObject tweet = (JSONObject) tuple.getValueByField("tweet");
        try {
            JSONArray hashtags = tweet.getJSONArray("hashtagEntities");

            // Iterate over all hashtags end emit
            for (int i = 0; i < hashtags.length(); i++) {
                String tag = hashtags.getString(i);
                _collector.emit(new Values(tag, tweet));
            }
            if (hashtags == null) {
                System.out.println("[INFO] Empty hashtags, nothing to emit.");
            }
        } catch (JSONException jsonerr) {
            System.err.println("[ERROR] in thread " + Thread.currentThread() + ": " + jsonerr.getMessage());
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
