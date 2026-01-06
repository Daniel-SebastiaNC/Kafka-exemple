package br.com.alura.ecommerce;

import br.com.alura.ecommerce.cosumer.ConsumerService;
import br.com.alura.ecommerce.cosumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService implements ConsumerService<Email> {
    private static final int THREADS = 1;

    public static void main(String[] args) {
        new ServiceRunner<>(EmailService::new).start(THREADS);
    }

    @Override
    public String getConsumerGroup() {
        return EmailService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_SEND_EMAIL";
    }

    public void parse(ConsumerRecord<String, Message<Email>> record) {
        System.out.println("-------------------------------------------");
        System.out.println("Sending email");
        System.out.println(record.key());
        System.out.println(record.value().getPayload());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println(record.timestamp());
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Email sent");
    }
}
