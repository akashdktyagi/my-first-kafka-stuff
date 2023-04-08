package org.example;

import org.apache.kafka.clients.producer.*;
import java.io.*;
import java.util.*;

public class ProducerMain {

    public static void main(String[] args) throws IOException, InterruptedException {
        ProducerMain producerMain = new ProducerMain();
        Properties props = producerMain.loadConfig("client.properties");

        //This is important, i these are not there , then it throws error
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        int counter=0;
        while (counter<1000){
            producer.send(new ProducerRecord<>("my-topic", UUID.randomUUID().toString(), "Akash Tyagi Score : " + counter));
            counter = counter + 1;
            Thread.sleep(1000);
        }

        producer.close();
    }

    public Properties loadConfig(final String configFile) throws IOException {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("client.properties");
        Properties properties = new Properties();
        properties.load(is);
        return properties;
    }
}