package com.kafkatest.App;

import com.kafkatest.verticles.ConsumerVerticle;
import com.kafkatest.verticles.ProducerVerticle;
import io.vertx.core.Vertx;

/**
 * Created by Sleeper on 16/01/2018.
 */
public class Launcher {

    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new ProducerVerticle());
        vertx.deployVerticle(new ConsumerVerticle());
    }
}
