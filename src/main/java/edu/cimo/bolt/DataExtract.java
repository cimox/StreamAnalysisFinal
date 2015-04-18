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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by cimo on 18/04/15.
 */
public class DataExtract implements IRichBolt {
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
            JSONObject tweet =(JSONObject) tuple.getValueByField("tweet");
            String hashtag = (String) tuple.getValueByField("hashtag");
            Timestamp.markWithTimestamp(tweet, "timestamp-data-extract");
            Map<String, Long> times = new HashMap<String, Long>();
            times.put("timestamp-enter", tweet.getLong("timestamp-enter"));
            times.put("timestamp-hashtag-extract", tweet.getLong("timestamp-hashtag-extract"));
            times.put("timestamp-filter", tweet.getLong("timestamp-filter"));
            times.put("timestamp-data-extract", tweet.getLong("timestamp-data-extract"));
            String created_at = tweet.getString("created_at");
            String id = tweet.getString("id_str"); // This need to be just id if data coming from live stream
            JSONObject userObject = tweet.getJSONObject("user");
            String user = userObject.getString("screen_name");
            String text = tweet.getString("text");

            // Emit all the shit...stuff.
            _collector.emit(new Values(hashtag, times, created_at, id, user, text));
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
        // Emit all recorded times as map
        outputFieldsDeclarer.declare(new Fields("hashtag", "times", "created_at", "id", "user", "text"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
