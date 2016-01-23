# learningkafka
CLI tool to listen on a kafka topic, also spawns a thread to generate random messages

## Requirements
1. JRE 6 or higher (JAVA_HOME set)
2. Maven (build)
4. Zookeeper
3. Kafka

## Build
`$ mvn package`

## Launch 
`$ java -jar target/kafka-test.jar <options> [kafkahost:port] [...]`  
or  
`./run.sh <options> [kafkahost:port] [...]`

### Options
- **-h** print help()
- **-n --topic** topic name
- **-l --loglevel** loglevel [fatal,severe,info,debug,trace]
