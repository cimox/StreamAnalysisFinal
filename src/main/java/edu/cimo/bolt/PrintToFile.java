package edu.cimo.bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import edu.cimo.util.Timestamp;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cimo on 08/03/15.
 */
public class PrintToFile implements IRichBolt {
    private Long _cnt;
    private HashMap<String, BufferedWriter> _files;
    private String _threadName;

    @Override
    public synchronized void execute(Tuple tuple) {
        Fields fields = tuple.getFields(); // Fields("hashtag", "times", "created_at", "id", "user", "text")
        String hashtag = (String) tuple.getValueByField("hashtag");

        try {
            if (!_files.containsKey(hashtag)) {
                // If file is not opened, then open it and save to list of opened files.
                openFile(hashtag);
            }


            printToFile(hashtag, tuple);
        } catch (NullPointerException nullErr) {
            System.err.println("[ERROR] in " + Thread.currentThread() + " " + nullErr.getMessage());
        } catch (IOException errIO) {
            System.err.println("[ERROR] in " + Thread.currentThread() + " " + errIO.getMessage());
        } catch (JSONException e) {
            System.err.println("[ERROR] in " + Thread.currentThread() + " " + e.getMessage());
        }
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
    }

    @Override
    public void cleanup() {
        System.out.println("--- [INFO] ---\n[" + _threadName + "]");
        System.out.println("msgs processed: " + _cnt);

        for (String query : _files.keySet()) {
            try {
                System.out.println("Closing file: ...[" + query + "].txt");
                _files.get(query).close();
            } catch (IOException e) {
                System.err.println("[ERROR] in " + Thread.currentThread() + " " + e.getMessage());
            }
        }
        System.out.println("--------------");
    }

    private synchronized void openFile(String query) throws IOException {
        FileWriter fileWriter = new FileWriter("/tmp/final-topology-" + query + ".csv", true);
        BufferedWriter writer = new BufferedWriter(fileWriter);
        writer.write("timestamp-enter,timestamp-exit,timestamp-hashtag-extract,timestamp-filter,timestamp-data-extract," +
                "total-time,created_at,id,user,text\n");
        _files.put(query, writer);
    }

    private synchronized void printToFile(String query, Tuple tweet) throws IOException, JSONException {
        HashMap<String, Long> times = (HashMap<String, Long>) tweet.getValueByField("times");
        long timeExit = System.nanoTime();
        long totalTime = timeExit - times.get("timestamp-enter");
        Long enterTime = times.get("timestamp-enter");

        // CSV format
        _files.get(query).write(
                "" + enterTime + ","
                + "" + timeExit + ","
                + "" + ((Long )times.get("timestamp-hashtag-extract") - enterTime) + ","
                + "" + ((Long )times.get("timestamp-filter") - enterTime) + ","
                + "" + ((Long )times.get("timestamp-data-extract") - enterTime) + ","
                + "" + totalTime + "," // total time
                + "" + tweet.getValueByField("created_at") + ","
                + "" + tweet.getValueByField("id") + ","
                + "" + tweet.getValueByField("user") + ","
                + "" + tweet.getValueByField("text").toString().replace("\n", " ") + ""
                + "\n");
        // Print tweet time in nanos, tweet ID : text
        // Format do Redis-u
//        _files.get(query).write("total-time[" + tweetTime + "],"
//                + "timestamp-enter[" + enterTime + "],"
//                + "timestamp-hashtag-extract[" + ((Long )times.get("timestamp-hashtag-extract") - enterTime) + "],"
//                + "timestamp-filter[" + ((Long )times.get("timestamp-filter") - enterTime) + "],"
//                + "timestamp-data-extract[" + ((Long )times.get("timestamp-data-extract") - enterTime) + "],"
//                + "created_at[" + tweet.getValueByField("created_at") + "],"
//                + "id[" + tweet.getValueByField("id") + "],"
//                + "user[" + tweet.getValueByField("user") + "],"
//                + "text[" + tweet.getValueByField("text") + "]"
//                + "\n");
        _cnt++;
    }
}
