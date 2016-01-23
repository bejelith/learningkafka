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
    private final String topic;

    public KafkaTopic(String topic, Integer streams){
        this.topic = topic;
        map = new HashMap<>();
        map.put(topic, streams);
    }

    public List<KafkaStream<byte[], byte[]>> getConsumerStreams(ConsumerConnector consumer){
        return consumer.createMessageStreams(map).get(topic);
    }

}
