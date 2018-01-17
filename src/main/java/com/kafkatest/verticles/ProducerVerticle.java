package com.kafkatest.verticles;


import io.vertx.core.AbstractVerticle;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Sleeper on 16/01/2018.
 */
public class ProducerVerticle extends AbstractVerticle {

    @Override
    public void start(){

        vertx.createHttpServer().requestHandler(
                r->{
                    if( r.getParam("text")!= null ) {
                        System.out.println("Value to produce: " +r.getParam("text"));
                        KafkaProducer<String,String> producer = createProducer();
                        KafkaProducerRecord<String, String> record =
                                KafkaProducerRecord.create("test", r.getParam("text"));
                        producer.write(record);
                    }
                 r.response().end("Received " +  r.getParam("text"));
                }).listen(8080);
    }

    private  KafkaProducer<String,String> createProducer(){
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("acks", "1");
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);
        return producer;
    }
}
