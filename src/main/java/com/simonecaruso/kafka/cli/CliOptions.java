package com.simonecaruso.kafka.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.cyclopsgroup.jcli.annotation.Argument;
import org.cyclopsgroup.jcli.annotation.Cli;
import org.cyclopsgroup.jcli.annotation.MultiValue;
import org.cyclopsgroup.jcli.annotation.Option;

@Cli(name = "kafka-test", description = "", note = "")
public class CliOptions {

    private List<String> arguments;
    private String topic;
    private boolean help = false;
    private String level = "info";
    private String operationlog = "stdout.log";
    private String zookeeperhosts = "localhost";

    @Option(name = "l", longName = "loglevel", description = "")
    public void setLogLevel(String level) {
        this.level = level;
    }

    public String getLogLevel() {
        return level;
    }

    @Option(name = "n", longName = "topic", description = "")
    public void setTopic(String topic){ this.topic = topic; }

    public String getTopic(){ return this.topic; }

    @Option(name = "z", longName = "zk", description = "")
    public void setZookeeperhosts(String zookeeperhosts) {
        this.zookeeperhosts = zookeeperhosts;
    }

    public String getZookeeperhosts() {
        return zookeeperhosts;
    }

    @Option(name = "h", longName = "help", description = "")
    public void setHelp() {
        help = true;
    }

    public boolean help() {
        return help;
    }

    @MultiValue(listType = ArrayList.class)
    @Argument(description = "Kafka broker list")
    public void setArguments(List<String> arg) {
        arguments = arg;
    }

    public List<String> getArguments() {
        return arguments;
    }


    /*
     * TODO Implement a nextToken() function?? I should..
     */
    private Properties parseProperties(String p) {
        char[] a = p.toCharArray();
        Properties props = new Properties();
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < a.length; i++) {
            if (a[i] == '\\') {
                if (a[i + 1] == ',') {
                    i++;
                    b.append(',');
                } else {
                    b.append(p.charAt(i));
                }
            } else if (a[i] == ',' || (i == a.length - 1) ) {
                if (a[i] != ',')
                    b.append(a[i]);
                String e = b.toString();
                int pos = e.indexOf('=');
                if (pos != -1 && pos != 0 && pos != e.length() - 1) {
                    props.put(e.substring(0, pos), e.substring(pos + 1));
                }
                b = new StringBuilder();
            } else {
                b.append(p.charAt(i));
            }
        }
        return props;
    }

}
