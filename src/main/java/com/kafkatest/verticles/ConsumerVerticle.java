package com.kafkatest.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import kafka.Kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by Sleeper on 16/01/2018.
 */
public class ConsumerVerticle extends AbstractVerticle{

    @Override
    public void start(){
        //Consumers with Subscribe
        KafkaConsumer<String,String> consumerSubscriber1 = createKafkaConsumer("my_group");

        consumerSubscriber1.handler(record -> {
            System.out.println("ConsumerSubscriber1: Processing key=" + record.key() + ",value=" + record.value() +
                    ",partition=" + record.partition() + ",offset=" + record.offset());
        });
        Set<String> topics = new HashSet<>();
        topics.add("test1");
        topics.add("test2");
        topics.add("repTopic");
        consumerSubscriber1.subscribe(topics);
     /*
     //Consumers with Assign
        KafkaConsumer<String,String> consumerAssigner1 = createKafkaConsumer("assignerGroup");
        consumerAssigner1.handler(record->{
            System.out.println("ConsumerAssigner1: Processing key=" + record.key() + ",value=" + record.value() +
                    ",partition=" + record.partition() + ",offset=" + record.offset());
        });
        Set<TopicPartition> topicPartitions = new HashSet<>();
        topicPartitions.add(new TopicPartition()
                .setTopic("repTopic")
                .setPartition(1));
        // registering handlers for assigned and revoked partitions
        consumerAssigner1.partitionsAssignedHandler(topicPartitionsInput -> {

            System.out.println("Partitions assigned");
            for (TopicPartition topicPartition : topicPartitionsInput) {
                System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
            }
        });

        consumerAssigner1.partitionsRevokedHandler(topicPartitionsInput -> {

            System.out.println("Partitions revoked");
            for (TopicPartition topicPartition : topicPartitionsInput) {
                System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
            }
        });
        consumerAssigner1.assign(topicPartitions,done -> {

            if (done.succeeded()) {
                System.out.println("Partition assigned");

                // requesting the assigned partitions
                consumerAssigner1.assignment(done1 -> {

                    if (done1.succeeded()) {

                        for (TopicPartition topicPartition : done1.result()) {
                            System.out.println(topicPartition.getTopic() + " " + topicPartition.getPartition());
                        }
                    }
                });
            }else{
                System.out.println("Failure during partitions assignement");
            }
        });

       /* KafkaConsumer<String,String> consumerAssigner2 = createKafkaConsumer("assignerGroup");
        consumerAssigner2.handler(record->{
            System.out.println("ConsumerAssigner2: Processing key=" + record.key() + ",value=" + record.value() +
                    ",partition=" + record.partition() + ",offset=" + record.offset());
        });
        consumerAssigner2.assign(topicPartitions);*/
    }

    private KafkaConsumer<String,String> createKafkaConsumer(String group){
        Map<String,String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9093,localhost:9094");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        if(group != null)
            config.put("group.id", group);
       // config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "false");

        return KafkaConsumer.create(vertx,config);
        }


}
