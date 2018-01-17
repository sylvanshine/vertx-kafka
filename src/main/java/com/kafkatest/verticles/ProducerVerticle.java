package com.kafkatest.verticles;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerRequest;
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
                    handleRequest(r);
                    r.response().end("Received " +  r.getParam("text"));
                }).listen(8080);
    }


    private void  handleRequest(HttpServerRequest request){
        if( request.getParam("text")!= null ) {
            System.out.println("Value to produce: " +request.getParam("text"));
            KafkaProducer<String,String> producer = createProducer();
            KafkaProducerRecord<String, String> record =
                    KafkaProducerRecord.create("test1", request.getParam("text") + " on test1");
            KafkaProducerRecord<String, String> record2 =
                    KafkaProducerRecord.create("test2",request.getParam("text") + " on test2");
            KafkaProducerRecord<String, String> recordPartitioned =
                    //KafkaProducerRecord.create("repTopic", "mykey1",request.getParam("text") + " on repTopic",1);
                    KafkaProducerRecord.create("repTopic", request.getParam("text") + " on repTopic");
            //producer.write(record);
            //producer.write(record2);
            producer.write(recordPartitioned);
        }
    }

    private  KafkaProducer<String,String> createProducer(){
        Map<String, String> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9093,localhost:9094");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //config.put("acks", "1");
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, config);
        return producer;


    }
}
