/**
 * Created by simone.caruso on 21/01/2016.
 */
package com.simonecaruso.kafka.worker;

import com.simonecaruso.kafka.KafkaTopic;
import com.simonecaruso.kafka.cli.CliOptions;
import org.apache.kafka.clients.producer.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerWorker implements Runnable {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final KafkaProducer producer;
    private final KafkaTopic topic;
    private boolean exit = true;

    public ProducerWorker(KafkaProducer producer, KafkaTopic topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public void run() {
        Thread.currentThread().setName("ProducerWorker");
        log.trace("Consumer started.");
        ProducerRecord<String, String> record;
        for (int i = 0; exit; i++) {
            String key = UUID.randomUUID().toString();
            record = new ProducerRecord<>(topic.getTopicName(), key, "testdata_" + i);
            Future<RecordMetadata> f = producer.send(record);

            try {
                RecordMetadata m = f.get();
               log.debug("offset: " + m.offset());
                Thread.sleep(1000); //Slow it down
            } catch (ExecutionException|InterruptedException e) {
                log.info(e)
                exit = true;
            }
        }
        producer.close();
        System.out.println("Producer exit");
    }

    private class ICallback<K> implements Callback {

        private K key;

        public ICallback(K key) {
            this.key = key;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (recordMetadata == null) {
                System.out.println("Error sending " + key.toString());
                e.printStackTrace();
            } else {
                System.out.println("Stored " + key.toString());
            }
        }
    }
}
