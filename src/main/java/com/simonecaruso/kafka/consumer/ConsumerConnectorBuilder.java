package com.simonecaruso.kafka.consumer;

import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Created by simone.caruso on 23/01/2016.
 */
public class ConsumerConnectorBuilder {

    private String zkconnect;
    private String boostrapservers;
    private String topic;
    private String gropupid;
    private String sessionTimeout;

    public ConsumerConnectorBuilder() {

    }

    public ConsumerConnectorBuilder withZk(String zkconnect) {
        this.zkconnect = zkconnect;
        return this;
    }

    public ConsumerConnectorBuilder withBootstrapServers(String bootstrapsers) {
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

    public ConsumerConnector build() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapservers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, gropupid);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS, sessionTimeout);
        return kafka.consumer.Consumer.createJavaConsumerConnector(new kafka.consumer.ConsumerConfig(props));
    }

}
