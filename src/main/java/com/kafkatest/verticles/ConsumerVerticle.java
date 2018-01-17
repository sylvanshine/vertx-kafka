package com.kafkatest.verticles;

import io.vertx.core.AbstractVerticle;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import kafka.Kafka;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sleeper on 16/01/2018.
 */
public class ConsumerVerticle extends AbstractVerticle{

    @Override
    public void start(){
        KafkaConsumer<String,String> consumer = createKafkaConsumer("my_group");

        consumer.handler(record -> {
            System.out.println("Processing key=" + record.key() + ",value=" + record.value() +
                    ",partition=" + record.partition() + ",offset=" + record.offset());
        });

        consumer.subscribe("test");
    }

    private KafkaConsumer<String,String> createKafkaConsumer(String group){
        Map<String,String> config = new HashMap<>();
        config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        config.put("group.id", group);
        config.put("auto.offset.reset", "earliest");
        config.put("enable.auto.commit", "false");

        return KafkaConsumer.create(vertx,config);
        }


}
