package br.com.alura.ecommerce;

import br.com.alura.ecommerce.cosumer.ConsumerService;
import br.com.alura.ecommerce.cosumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

public class ReadingReportService implements ConsumerService<User> {
    private static final Path SOURCE = new File("C:\\Users\\Daniel\\IdeaProjects\\ecommerce\\service-reading-report\\src\\main\\resources\\report.txt").toPath();
    private static final int THREADS = 1;
    public static void main(String[] args)  {
        new ServiceRunner<>(ReadingReportService::new).start(THREADS);
    }

    public void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("-------------------------------------------");
        System.out.println("Processing report for " + record.value());

        var message = record.value();
        var user = (User) message.getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " +user.getUuid());

        System.out.println("File created: " + target.getAbsolutePath());
    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }
}
