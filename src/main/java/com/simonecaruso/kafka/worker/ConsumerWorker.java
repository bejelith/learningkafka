/**
 * Created by simone.caruso on 21/01/2016.
 */
package com.simonecaruso.kafka.worker;

import com.simonecaruso.kafka.KafkaTopic;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.List;

public class ConsumerWorker implements Runnable {

    private final KafkaTopic topic;
    private ConsumerConnector consumer;
    private boolean run = true;
    private int processedmessages = 0;

    public ConsumerWorker(ConsumerConnector consumer, KafkaTopic topic) {
        this.consumer = consumer;
        this.topic = topic;
    }

    public void run() {
        System.out.println("Consumer started.");//TODO write loggin class
        List<KafkaStream<byte[], byte[]>> streams = topic.getConsumerStreams(consumer);
        for (KafkaStream<byte[], byte[]> stream : streams) {
            ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
            while (run && iterator.hasNext()) {
                processedmessages++;
                MessageAndMetadata<byte[], byte[]> message = iterator.next();
                System.out.println("Receved: " + new String(message.key()) + ": " + new String(message.message()));

                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    System.out.println("Interrupting");
                    return;
                }
            }
        }
    }

    public void exit() {
        run = false;
    }

    public int getProcessedmessages() {
        return processedmessages;
    }
}
