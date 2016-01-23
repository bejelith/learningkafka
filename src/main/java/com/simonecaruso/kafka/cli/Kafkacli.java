/**
 * Created by simone.caruso on 20/01/2016.
 */
package com.simonecaruso.kafka.cli;

import com.simonecaruso.kafka.KafkaTopic;
import com.simonecaruso.kafka.cli.CliOptions;
import com.simonecaruso.kafka.worker.ConsumerWorker;
import com.simonecaruso.kafka.worker.ProducerWorker;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.*;
import org.cyclopsgroup.jcli.ArgumentProcessor;
import org.cyclopsgroup.jcli.GnuParser;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.simonecaruso.kafka.consumer.ConsumerConnectorBuilder;


public class Kafkacli {

    private final static CliOptions clioptions = new CliOptions();
    private final static ArgumentProcessor<CliOptions> argprocessor =
            ArgumentProcessor.newInstance(CliOptions.class, new GnuParser());
    private final static Logger log = LogManager.getLogger("com.simonecaruso.kafka");

    public static void main(String args[]){
        //process cli arguments
        parseArguments(args);
        setupLogger(clioptions.getLogLevel());

        //Create the topic instance
        KafkaTopic topic = new KafkaTopic(clioptions.getTopic(), 1);

        //Create the consumer instance
        ConsumerConnector consumerConnector = new ConsumerConnectorBuilder()
                .withBootstrapServers(clioptions.getArguments())
                .withZk(clioptions.getZookeeperhosts())
                .build();

        //Produce random stuff
        ExecutorService executor = Executors.newCachedThreadPool();
        executor.execute(new ProducerWorker(clioptions));

        //Consume the random stuff
        executor.execute(new ConsumerWorker(consumerConnector, topic));
        executor.shutdown();

        try {
            while(!executor.awaitTermination(100, TimeUnit.MILLISECONDS)){

            }
        } catch (InterruptedException e) {
            System.out.println("Termination signal catched.");
        }
        System.out.println("Exit.");
    }

    private static void parseArguments(String[] args){
        argprocessor.process(args, clioptions);
        if(clioptions.help() || clioptions.getArguments().size() < 1){
            try {
                argprocessor.printHelp(new PrintWriter(System.out, true));
            } catch (IOException e) {
                //do not care for stdout
            }
            System.exit(-1);
        }
    }

    private static void setupLogger(String level){
        Appender console = new ConsoleAppender(new PatternLayout("%d %-5p [%t]: %m%n"));
        Logger root = LogManager.getRootLogger();
        root.setLevel(Level.ERROR);
        log.addAppender(console);
    }

}
