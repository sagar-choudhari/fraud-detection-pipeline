package com.frauddetection.producer;


import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.frauddetection.model.Transaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TransactionProducer {

    private static final String TOPIC = "transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);

        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        try(KafkaProducer<String, String> producer = new KafkaProducer<>(props)){
            for (int i=0; i < 20; i++){
                Transaction txn = Transaction.createRandom();
                String json = mapper.writeValueAsString(txn);

                ProducerRecord<String, String> record =new ProducerRecord<>(TOPIC, txn.getTxnId(), json);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending record: " + exception.getMessage());
                    } else {
                        System.out.println("Sent → partition=" + metadata.partition()
                                + " offset=" + metadata.offset()
                                + " txnId=" + txn.getTxnId());
                    }
                });

                Thread.sleep(500);
            }

        }

        System.out.println("All transactions sent.");

    }
}
