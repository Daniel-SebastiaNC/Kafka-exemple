package br.com.alura.ecommerce.cosumer;

import br.com.alura.ecommerce.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {
    void consumer(ConsumerRecord<String, Message<T>> record) throws Exception;
}
