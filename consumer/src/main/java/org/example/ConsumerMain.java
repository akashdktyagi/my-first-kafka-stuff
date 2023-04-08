package org.example;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


public class ConsumerMain {
    public static void main(String[] args) throws IOException {

        ConsumerMain consumerMain = new ConsumerMain();
        Properties props = consumerMain.loadConfig("client.properties");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("my-topic"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("key = %s, value = %s%n", record.key(), record.value());
            }
        }
    }

    public Properties loadConfig(final String configFile) throws IOException {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("client.properties");
        Properties properties = new Properties();
        properties.load(is);
        return properties;
    }




}