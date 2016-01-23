/**
 * Created by simone.caruso on 21/01/2016.
 */
package com.simonecaruso.kafka.worker;

import com.simonecaruso.kafka.cli.CliOptions;
import org.apache.kafka.clients.producer.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerWorker implements Runnable {

    private final CliOptions options;
    private boolean exit = true;

    public ProducerWorker(CliOptions options){
        this.options = options;
    }



    public void run() {
        System.out.println("Producer started");
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"Producer1");
        props.put(ProducerConfig.TIMEOUT_CONFIG, "50");
        props.put(ProducerConfig.RETRIES_CONFIG, "1");


        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> record;
        for(int i = 0; exit; i++){
            String key = UUID.randomUUID().toString();
            record = new ProducerRecord<>(options.getTopic(), key, "testdata_" + i);
            Future<RecordMetadata> f = producer.send(record);

            try {
                RecordMetadata m = f.get();
                System.out.println("offset: "+ m.offset());
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
                exit = true;
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
        System.out.println("Producer exit");
    }

    private class ICallback<K> implements Callback {

        private K key;

        public ICallback(K key){
            this.key = key;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if(recordMetadata == null) {
                System.out.println("Error sending " + key.toString());
                e.printStackTrace();
            }else{
                System.out.println("Stored " + key.toString());
            }
        }
    }
}
