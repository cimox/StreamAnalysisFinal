package bolt;


import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by cimo on 08/03/15.
 */
public class PrintToFile implements IRichBolt {
    private Long _cnt;
    private HashMap<String, BufferedWriter> _files;

    @Override
    public synchronized void execute(Tuple tuple) {
        String tweet = (String) tuple.getValueByField("tweet");

        try {
            if (!_files.containsKey(null)) {
                // If file is not opened, then open it and save to list of opened files.
                openFile(null);
            }
            System.out.println(-1.0 + " " + tweet);
            printToFile(null, tweet, -1.0);
        } catch (NullPointerException nullErr) {
            System.err.println("[ERROR] in " + Thread.currentThread() + " " + nullErr.getMessage());
        } catch (IOException errIO) {
            System.err.println("[ERROR] in " + Thread.currentThread() + " " + errIO.getMessage());
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
    }

    @Override
    public void cleanup() {
        System.out.println("-------\n[" + Thread.currentThread() + "]");
        System.out.println("msgs processed: " + _cnt);

        for (String query : _files.keySet()) {
            try {
                System.out.println("Closing file: ...[" + query + "].txt");
                _files.get(query).close();
            } catch (IOException e) {
                System.err.println("[ERROR] in " + Thread.currentThread() + " " + e.getMessage());
            }
        }
        System.out.println("-------\n");
    }

    private synchronized void openFile(String query) throws IOException {
        FileWriter fileWriter = new FileWriter("/tmp/basic-first-topology-" + query + ".txt", true);
        BufferedWriter writer = new BufferedWriter(fileWriter);
        _files.put(query, writer);
    }

    private synchronized void printToFile(String query, String tweet, Double tweetTime) throws IOException {

            // Print tweet time in nanos, tweet ID : text
//            _files.get(query).write("[" + tweetTime + "] " + tweet.getString("id_str") + ":" + tweet.getString("text") + "\n");
            _files.get(query).write("[" + tweetTime + "] " + tweet + "\n");
            _cnt++;
    }
}
