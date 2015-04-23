package edu.cimo.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.cimo.util.Timestamp;
import redis.clients.jedis.Jedis;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cimo on 08/03/15.
 */
public class PrintToRedis implements IRichBolt {
    private Long _cnt;
    private HashMap<String, BufferedWriter> _files;
    private String _threadName;
    private Jedis _jedis;

    @Override
    public synchronized void execute(Tuple tuple) {
        String hashtag = (String) tuple.getValueByField("hashtag");

//        System.out.println("[INFO] Printing to Redis in " + _threadName + " > [" + hashtag + "] ");
        _jedis.sadd(hashtag, constructText(tuple));

        // CSV format
//        _files.get(query).write(
//                "\"" + (String) tuple.getValueByField("timestamp") + "\","
//                        + "\"" + enterTime + "\","
//                        + "\"" + timeExit + "\","
//                        + "\"" + timestampTime + "\","
//                        + "\"" + hashtagExtractTime + "\","
//                        + "\"" + filterTime + "\","
//                        + "\"" + dataExtractTime + "\","
//                        + "\"" + totalTime + "\"," // total time
//                        + "\"" + tuple.getValueByField("created_at") + "\","
//                        + "\"" + tuple.getValueByField("id") + "\","
//                        + "\"" + tuple.getValueByField("user") + "\","
//                        + "\"" + tuple.getValueByField("text").toString().replaceAll("\\r?\\n", " --linebreak-- ") + "\""
//                        + "\n");
    }

    private String constructText(Tuple tweet) {
        StringBuilder constructedText = new StringBuilder();


        // All measured times on tweet
        HashMap<String, Long> times = (HashMap<String, Long>) tweet.getValueByField("times");
        Long timestampExitNanos = System.nanoTime();
        Long timestampEnterNanos = times.get("timestamp-enter");
        Long totalTime = timestampExitNanos - timestampEnterNanos;
        Long timestampTimeNanos = ((Long )times.get("timestamp-hashtag-extract") - timestampEnterNanos);
        Long hashtagExtractTimeNanos = (Long )times.get("timestamp-filter") - (Long )times.get("timestamp-hashtag-extract");
        Long filterTimeNanos = (Long )times.get("timestamp-data-extract") - (Long )times.get("timestamp-filter");
        Long dataExtractTimeNanos = timestampExitNanos - ((Long )times.get("timestamp-data-extract"));
        String text = (String) tweet.getValueByField("text");

        // Get data and construct text
        // start-timestamp-milis,timestamp-enter-nanos,timestamp-exit-nanos,timestamp-nanos,timestamp-hashtag-extract-nanos,
        // timestamp-filter-nanos,timestamp-data-extract-nanos,total-time-nanos,created_at,id,user,text
        constructedText.append((String) tweet.getValueByField("timestamp") + ",");
        constructedText.append(timestampEnterNanos + "," + timestampExitNanos + ",");
        constructedText.append(timestampTimeNanos + "," + hashtagExtractTimeNanos + ","
                + filterTimeNanos + "," + dataExtractTimeNanos + "," + totalTime + ",");
        constructedText.append((String) tweet.getValueByField("created_at") + ",");
        constructedText.append((String) tweet.getValueByField("id") + ",");
        constructedText.append((String) tweet.getValueByField("user") + ",");
        constructedText.append(text.replace(",", " comma-removed ").replace("\n", ""));

        return constructedText.toString();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // Nothing to emit.
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _cnt = new Long(0);
        _files = new HashMap<String, BufferedWriter>(1);
        _threadName = Thread.currentThread().getName();
        _jedis = new Jedis("localhost");
    }

    @Override
    public void cleanup() {
        System.out.println("--- [INFO] ---\n[" + _threadName + "]");
        System.out.println("msgs processed: " + _cnt);
        System.out.println("--------------");
    }

}
