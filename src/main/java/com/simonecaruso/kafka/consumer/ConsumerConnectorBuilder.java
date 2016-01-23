package com.simonecaruso.kafka.consumer;

import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import java.util.List;
import java.util.LinkedList;
import java.util.Properties;

/**
 * Created by simone.caruso on 23/01/2016.
 */
public class ConsumerConnectorBuilder {

    private String zkconnect = "localhost";
    private List<String> boostrapservers = new LinkedList<>();
    private String topic = "testtopic";
    private String gropupid = "group1";
    private String sessionTimeout = "100";

    public ConsumerConnectorBuilder() {


    }

    public ConsumerConnectorBuilder withZk(String zkconnect) {
        this.zkconnect = zkconnect;
        return this;
    }

    public ConsumerConnectorBuilder withBootstrapServers(List<String> bootstrapsers) {
        this.boostrapservers = boostrapservers;
        return this;
    }

    public ConsumerConnectorBuilder withTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public ConsumerConnectorBuilder withGroupId(String groupId) {
        this.gropupid = groupId;
        return this;
    }

    public ConsumerConnectorBuilder addServer(String server){
        boostrapservers.add(server);
        return this;
    }

    public ConsumerConnector build() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, StringUtils.join(boostrapservers.iterator(), ','));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, gropupid);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS, sessionTimeout);
        props.put("zookeeper.connect", zkconnect);
        return kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(props));
    }

}
