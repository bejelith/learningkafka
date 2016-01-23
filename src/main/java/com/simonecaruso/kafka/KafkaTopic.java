package com.simonecaruso.kafka;

import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Created by simone.caruso on 23/01/2016.
 */
public class KafkaTopic {

    private final Map<String, Integer> map;
    private final String topicName;

    public KafkaTopic(String topicName, Integer streams){
        this.topicName = topicName;
        map = new HashMap<>();
        map.put(topicName, streams);
    }

    public List<KafkaStream<byte[], byte[]>> getConsumerStreams(ConsumerConnector consumer){
        try {
            return consumer.createMessageStreams(map).get(topicName);
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public String getTopicName() {
        return topicName;
    }
}
