/**
 * Created by simone.caruso on 21/01/2016.
 */
package com.simonecaruso.kafka.worker;

import com.simonecaruso.kafka.KafkaTopic;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ConsumerWorker implements Runnable {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final KafkaTopic topic;
    private ConsumerConnector consumer;
    private boolean run = true;
    private int processedmessages = 0;

    public ConsumerWorker(ConsumerConnector consumer, KafkaTopic topic) {
        this.consumer = consumer;
        this.topic = topic;
    }

    public void run() {
        Thread.currentThread().setName("ConsumerWorker");
        log.trace("Consumer started.");
        KafkaStream<byte[], byte[]> stream = topic.getConsumerStreams(consumer).get(0);

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        try {
            while (run && iterator.hasNext()) {
                processedmessages++;
                MessageAndMetadata<byte[], byte[]> message = iterator.next();

                log.info("Message offset: " + message.offset() + ": " + message.message());
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    log.info("Interrupting");
                    return;
                }
            }
        } catch (ConsumerTimeoutException e) {
            log.error(e);
        }
        log.trace("Consumer shut down.");
        consumer.shutdown();
    }

    public void exit() {
        run = false;
    }

    public int getProcessedmessages() {
        return processedmessages;
    }
}
