package br.com.alura.ecommerce;

import br.com.alura.ecommerce.cosumer.ConsumerService;
import br.com.alura.ecommerce.cosumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }

    private final KafkaDispatcher<Email> emailDispatcher = new KafkaDispatcher<>();

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("-------------------------------------------");
        System.out.println("Processing new order, preparing email");
        var message = record.value();

        System.out.println(message);

        var order = (Order) message.getPayload();
        var id = message.getId().continueWith(EmailNewOrderService.class.getSimpleName());
        Email emailCode = new Email("New Order", "Thank you for your order! We are processing your order!");

        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), emailCode, id);

    }
}
