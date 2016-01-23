package com.simonecaruso.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by simone.caruso on 23/01/2016.
 */
public class ProducerBuilder {

    Map<String, Object> props = new HashMap<>();
    List<String> servers = new LinkedList<>();

    public ProducerBuilder(){
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"Client1");
        props.put(ProducerConfig.TIMEOUT_CONFIG, "5000");
        props.put(ProducerConfig.RETRIES_CONFIG, "1");
    }

    public ProducerBuilder withServers(List<String> servers){
        this.servers = servers;
        return this;
    }

    public ProducerBuilder addServer(String server){
        servers.add(server);
        return this;
    }

    public ProducerBuilder withClientID(String clientID){
        props.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
        return this;
    }

    public KafkaProducer build(){
        return new KafkaProducer<>(props);
    }
}
