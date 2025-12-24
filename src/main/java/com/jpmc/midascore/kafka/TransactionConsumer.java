package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransactionConsumer {

    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "midas-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(Transaction transaction) {
        // Task 2 only checks that we can receive the message
        System.out.println("Received transaction: " + transaction);
    }
}
