package com.simonecaruso.kafka.test;

import com.simonecaruso.kafka.KafkaTopic;
import com.simonecaruso.kafka.consumer.ConsumerConnectorBuilder;
import com.simonecaruso.kafka.producer.KafkaProducerBuilder;
import com.simonecaruso.kafka.test.embedded.EmbeddedKafkaCluster;
import com.simonecaruso.kafka.test.embedded.EmbeddedZookeeper;
import com.simonecaruso.kafka.worker.ConsumerWorker;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

/**
 * Created by simone.caruso on 23/01/2016.
 */
public class IntegrationTest {

    static EmbeddedZookeeper embeddedZookeeper;
    static EmbeddedKafkaCluster embeddedKafkaCluster;

    private static volatile int zookeeperPort;

    private static volatile int karfkaPort;

    private final KafkaTopic topic = new KafkaTopic("testtopic", 1);

    private Thread thread;

    @BeforeClass
    public static void beforeClass() {
        zookeeperPort = 23000;
        karfkaPort = 23001;

        embeddedZookeeper = new EmbeddedZookeeper(zookeeperPort);
        List<Integer> kafkaPorts = new ArrayList<>();
        // -1 for any available port
        kafkaPorts.add(karfkaPort);
        embeddedKafkaCluster = new EmbeddedKafkaCluster(embeddedZookeeper.getConnection(), new Properties(), kafkaPorts);
        try {
            embeddedZookeeper.startup();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("### Embedded Zookeeper connection: " + embeddedZookeeper.getConnection());
        embeddedKafkaCluster.startup();
        System.out.println("### Embedded Kafka cluster broker list: " + embeddedKafkaCluster.getBrokerList());
    }

    @AfterClass
    public static void afterClass() {
        embeddedKafkaCluster.shutdown();
        embeddedZookeeper.shutdown();
    }

    @Test(timeout = 99000)
    public void mytest() throws Exception {
        ProducerRecord<String, String> record;
        embeddedKafkaCluster.createTopics(topic.getTopicName());
        ConsumerConnector consumer = new ConsumerConnectorBuilder()
                .addServer("localhost:" + karfkaPort)
                .withZk("localhost:" + zookeeperPort)
                .withSessionTimeout("50")
                .withGroupId("g1")
                .withTopic(topic.getTopicName())
                .build();
        KafkaProducer<String, String> producer = new KafkaProducerBuilder()
                .addServer("localhost:" + karfkaPort)
                .withClientID("producer1")
                .build();

        String[] messages = new String[]{"message1", "message2", "message3"};

        ConsumerWorker worker = new ConsumerWorker(consumer, topic);

        thread = new Thread(worker);
        thread.start();
        //Access linearization
        Thread.sleep(2500);

        for (int i = 0; i < messages.length; i++) {
            record = new ProducerRecord<>(topic.getTopicName(), Integer.toString(i), messages[i]);
            Future<RecordMetadata> f = producer.send(record);
            try {
                f.get();
            } catch (InterruptedException|ExecutionException e) {
                e.printStackTrace();
                throw e;
            }
        }


        worker.exit();
        try {
            Thread.sleep(1000);
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(messages.length, worker.getProcessedmessages());
    }

}
