package edu.cimo.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import edu.cimo.util.Timestamp;
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
    private Map<String, Long> _counters;
    private String _threadName;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        _queries = new HashMap<String, String>();
        _counters = new HashMap<String, Long>();
        _threadName = Thread.currentThread().getName();
    }

    @Override
    public void execute(Tuple tuple) {
        // To this bolt enters query, hashtag - tweet tuple
        Fields fields = tuple.getFields();

        if (fields.contains("query")) {
            // Persist query.
            String query = (String) tuple.getValueByField("query");
            _queries.put(query.toLowerCase(), query);
        }
        else if (fields.contains("hashtag") && fields.contains("tweet")) {
            String hashtag = (String) tuple.getValueByField("hashtag");
            JSONObject tweet = (JSONObject) tuple.getValueByField("tweet");

            try {
                if (_queries.containsKey(hashtag.toLowerCase())) {
                    Timestamp.markWithTimestamp(tweet, "timestamp-filter");
                    System.out.println("[INFO] MATCH filter [" + hashtag + "]! in " + _threadName + " > " + tweet.getString("text").replace("\n", " "));

                    // Emit hashtag/query and tweet and increment counter
                    _collector.emit(new Values(hashtag, tweet));

                    // Update counters
                    if (!_counters.containsKey(hashtag)) {
                        _counters.put(hashtag, new Long(1));
                    }
                    else {
                        Long cntIncr = _counters.get(hashtag);
                        _counters.put(hashtag, ++cntIncr);
                    }
                } else { // Nothing to emit, no hashtags matched with query
//                    System.out.println("[INFO] DID NOT MATCH " + Thread.currentThread() + " > " + tweet.getString("text"));
                }
            } catch (JSONException jsonerr) {
                System.err.println("[ERROR] in thread " + Thread.currentThread() + ": " + jsonerr.getMessage());
            }
        }
        _collector.ack(tuple);
    }

    @Override
    public void cleanup() {
        System.out.println("--- [INFO] ---\n[" + _threadName + "]");
        for (Map.Entry<String, Long> e : _counters.entrySet()) {
            System.out.println("For [" + e.getKey() + "] match [" + e.getValue() + "] times.");
        }
        System.out.println("--------------");
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
