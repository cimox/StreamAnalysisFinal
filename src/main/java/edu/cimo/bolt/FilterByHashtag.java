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

import java.util.HashMap;
import java.util.Map;

/**
 * Created by cimo on 18/04/15.
 */
public class FilterByHashtag implements IRichBolt {
    private OutputCollector _collector;
    private Map<String, String> _queries;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        _queries = new HashMap<String, String>();
    }

    @Override
    public void execute(Tuple tuple) {
        // To this bolt enters query, hashtag - tweet tuple
        Fields fields = tuple.getFields();

        if (fields.contains("query")) {
            // Query from spout came, persist it.
            String query = (String) tuple.getValueByField("query");
            _queries.put(query, query);
        }
        else if (fields.contains("hashtag") && fields.contains("tweet")) {
            String hashtag = (String) tuple.getValueByField("hashtag");
            JSONObject tweet = (JSONObject) tuple.getValueByField("tweet");

            try { // Basically all tweets here, already should match query.
                if (_queries.containsKey(hashtag)) {
                    // Emit hashtag/query and tweet
                    _collector.emit(new Values(hashtag, tweet));

                } else { // Nothing to emit, no hashtags matched with query
                    System.out.println("[INFO] DID NOT MATCH>> " + tweet.getString("text"));
                }
            } catch (JSONException jsonerr) {
                System.err.println("[ERROR] in thread " + Thread.currentThread() + ": " + jsonerr.getMessage());
            }
        }
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
