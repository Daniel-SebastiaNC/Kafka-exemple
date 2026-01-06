package br.com.alura.ecommerce.cosumer;

import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.dispatcher.GsonSerializer;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, Message<T>> consumer;
    private final ConsumerFunction<T> parse;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String,String> overrideProperties) {
        this(groupId, parse, overrideProperties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String,String> overrideProperties) {
        this(groupId, parse, overrideProperties);
        consumer.subscribe(topic);
    }

    private KafkaService(String groupId, ConsumerFunction<T> parse, Map<String,String> overrideProperties) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(getProperties(groupId, overrideProperties));
    }

    public void run() throws ExecutionException, InterruptedException {
        try (var deadLetter = new KafkaDispatcher<>()) {
            while (true) {
                var records = consumer.poll(Duration.ofMillis(100));

                if (!records.isEmpty()) {
                    for (var record : records) {
                        try {
                            this.parse.consumer(record);
                        } catch (Exception e) {
                            e.printStackTrace();
                            var message = record.value();
                            deadLetter.send("ECOMMERCE_DEADLETTER", message.getId().toString(), new GsonSerializer().serialize("", message), message.getId().continueWith("DeadLetter"));
                        }
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Map<String, String > overrideProperties) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.putAll(overrideProperties);
        return properties;
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
