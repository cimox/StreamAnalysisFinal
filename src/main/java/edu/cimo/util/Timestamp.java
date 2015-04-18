package edu.cimo.util;

import twitter4j.JSONException;
import twitter4j.JSONObject;

/**
 * Created by cimo on 18/04/15.
 */
public abstract class Timestamp {
    /**
     * Marks tweet with a timestamp, the time when record/tweet essentially entered application.
     * @param tweet String line to be timestamped.
     * @param attribute Name of timestamp attribute.
     * @return Timestamped string line.
     */
    public static synchronized void markWithTimestamp(JSONObject tweet, String attribute) throws JSONException {
        tweet.put(attribute, System.nanoTime());
    }
}
