# learningkafka
CLI tool to listen on a kafka topic, also spawns a thread to generate random messages

## Requirements
1. JRE 6 or higher (JAVA_HOME set)
2. Maven (build)
4. Zookeeper
3. Kafka

## Build
`$ mvn assembly:single`

## Build and run integration test
`$ mvn package`

## Launch 
`$ java -jar target/kafka-test.jar <options> [kafkahost:port] [...]`  


### Options
- **-h** print help()
- **-n --topic** [topic name]
- **-l --loglevel** [loglevel] set log level (fatal,severe,info,debug,trace)
- **-z --zk** [ZK connect string] ZK connection string
