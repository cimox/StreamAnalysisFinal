package edu.cimo.spout;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.*;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;


/**
 * Created by cimo on 15/03/15.
 */
public class TwitterLiveSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private LinkedBlockingQueue<Status> queue = null;
    private TwitterStream _twitterStream;
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
    private String[] keyWords;
    private Integer emittedTweetsCnt;

    public TwitterLiveSpout(String consumerKey, String consumerSecret,
                            String accessToken, String accessTokenSecret, String[] keyWords) {
        this.consumerKey = consumerKey;
        this.consumerSecret = consumerSecret;
        this.accessToken = accessToken;
        this.accessTokenSecret = accessTokenSecret;
        this.keyWords = keyWords;
        this.emittedTweetsCnt = new Integer(0);
    }

    public TwitterLiveSpout() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        queue = new LinkedBlockingQueue<Status>(1000);
        _collector = collector;

        StatusListener listener = new StatusListener() {

            @Override
            public void onStatus(Status status) {

                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
            }

            @Override
            public void onTrackLimitationNotice(int i) {
            }

            @Override
            public void onScrubGeo(long l, long l1) {
            }

            @Override
            public void onException(Exception ex) {
            }

            @Override
            public void onStallWarning(StallWarning arg0) {
                // TODO Auto-generated method stub

            }

        };

        // Preparing twitter stream
        _twitterStream = new TwitterStreamFactory(
                new ConfigurationBuilder().setJSONStoreEnabled(true).build())
                .getInstance();

        _twitterStream.addListener(listener);
        _twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
        AccessToken token = new AccessToken(accessToken, accessTokenSecret);
        _twitterStream.setOAuthAccessToken(token);


        if (keyWords == null) {

            _twitterStream.sample();
        }
        else {

            FilterQuery query = new FilterQuery().track(keyWords);
            _twitterStream.filter(query);
        }

    }

    @Override
    public void nextTuple() {
        // Emits tweet as a JSON Object
        Status tweet = queue.poll();

        if (tweet == null) {
            Utils.sleep(50);
        } else {
            JSONObject tweetToEmit = new JSONObject(tweet);
            try {
                String tweetText = tweetToEmit.getString("text");
                _collector.emit(new Values(tweetText));
                emittedTweetsCnt++;
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() {
        _twitterStream.shutdown();
        System.out.println("[TOTAL TWEETS EMITTED] " + emittedTweetsCnt);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);
        return ret;
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweet"));
    }

}
