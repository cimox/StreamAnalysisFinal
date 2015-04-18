package edu.cimo.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.util.Map;

/**
 * Created by cimo on 10/03/15.
 */
public class EnterTopologyTimestamp implements IRichBolt {
    private OutputCollector _collector;
    private Long _cnt;
    private String _threadName;

    @Override
    public void execute(Tuple tuple) {
        try {
            JSONObject tweetJSON = new JSONObject((String) tuple.getValueByField("tweet"));
            markWithTimestamp(tweetJSON, "timestamp-enter");
            _cnt++;

            _collector.emit(new Values(tweetJSON));
            _collector.ack(tuple);
        } catch (JSONException e) {
            System.err.println("[ERROR] in " + Thread.currentThread() + ": " + e.getMessage());
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        _cnt = new Long(0);
        _threadName = Thread.currentThread().getName();
    }

    @Override
    public void cleanup() {
        System.out.println("--- [INFO] ---\n[" + _threadName + "]");
        System.out.println("Messages entered: " + _cnt);
        System.out.println("--------------");
    }

    /**
     * Marks tweet with a timestamp, the time when record/tweet essentially entered application.
     * @param tweet String line to be timestamped.
     * @param attribute Name of timestamp attribute.
     * @return Timestamped string line.
     */
    private synchronized void markWithTimestamp(JSONObject tweet, String attribute) throws JSONException {
        tweet.put(attribute, System.nanoTime());

        /**
         * Old implementation with strings.
         * return tweet.insert(1, "\"" + attribute + "\":" + "\"" + System.currentTimeMillis() + "\",").toString();
         */
    }
}
