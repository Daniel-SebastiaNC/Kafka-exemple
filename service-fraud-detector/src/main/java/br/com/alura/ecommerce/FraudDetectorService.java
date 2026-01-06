package br.com.alura.ecommerce;

import br.com.alura.LocalDatabase;
import br.com.alura.ecommerce.cosumer.ConsumerService;
import br.com.alura.ecommerce.cosumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {

    private final LocalDatabase database;

    FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        this.database.createIfNotExists("create table Orders (" +
                "uuid varchar(200) primary key," +
                "is_fraud boolean)");
    }

    public static void main(String[] args) {
        new ServiceRunner<>(FraudDetectorService::new).start(1);
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("-------------------------------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        System.out.println(record.timestamp());

        var order = (Order) record.value().getPayload();

        if(wasProcessed(order)) {
            System.out.println("Order " + order.getOrderIdId() +" already processed");
            return;
        }

        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Order processed");

        if (isFraud(order)) {
            database.update("insert into Orders (uuid, is_fraud) values (?, true)", order.getOrderIdId());
            System.out.println("Order is a fraud!!! " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(), order,  record.value().getId().continueWith(FraudDetectorService.class.getSimpleName()));
        } else {
            database.update("insert into Orders (uuid, is_fraud) values (?, false)", order.getOrderIdId());
            System.out.println("Approved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(), order,  record.value().getId().continueWith(FraudDetectorService.class.getSimpleName()));
        }
    }

    private boolean wasProcessed(Order order) throws SQLException {
        ResultSet result = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderIdId());
        return result.next();
    }

    private static boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
