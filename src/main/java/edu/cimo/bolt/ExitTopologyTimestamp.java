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
 * Created by cimo on 09/03/15.
 */
public class ExitTopologyTimestamp implements IRichBolt {
    OutputCollector _collector;

    @Override
    public synchronized void execute(Tuple tuple) {
        JSONObject tweet = (JSONObject) tuple.getValueByField("tweet");
        String query = (String) tuple.getValueByField("query");

        try {
            markWithTimestamp(tweet, "exited");
        } catch (JSONException e) {
            System.err.println("[ERROR IN] " + Thread.currentThread() + " " + e.getMessage());
        }

        _collector.emit(new Values(tweet, query));
        _collector.ack(tuple);
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("tweet", "query"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {

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
