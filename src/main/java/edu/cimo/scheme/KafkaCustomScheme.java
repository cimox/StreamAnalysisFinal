package edu.cimo.scheme;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.kafka.StringScheme;

import java.io.UnsupportedEncodingException;
import java.util.List;


/**
 * Created by cimo on 18/04/15.
 */
public class KafkaCustomScheme extends StringScheme {

    private String STRING_SCHEME_KEY;

    public KafkaCustomScheme(String scheme_key) {
        this.STRING_SCHEME_KEY = scheme_key;
    }

    public List<Object> deserialize(byte[] bytes) {
        return new Values(deserializeString(bytes));
    }

    public static String deserializeString(byte[] string) {
        try {
            return new String(string, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields getOutputFields() {
        return new Fields(STRING_SCHEME_KEY);
    }
}
