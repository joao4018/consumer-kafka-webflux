package com.webflux.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerApplication {
    private static final Logger log = LoggerFactory.getLogger(ConsumerApplication.class.getName());

//    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
//    private static final String TOPIC = "demo-topic";
//
//    private final ReceiverOptions<Integer, String> receiverOptions;
//    private final DateTimeFormatter dateFormat;
//
//    public ConsumerApplication(String bootstrapServers) {
//
//        Map<String, Object> props = new HashMap<>();
//        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "sample-consumer");
//        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sample-group");
//        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        receiverOptions = ReceiverOptions.create(props);
//        dateFormat = DateTimeFormatter.ofPattern("HH:mm:ss:SSS z dd MMM yyyy");
//    }
//
//    public Disposable consumeMessages(String topic, CountDownLatch latch) {
//
//        ReceiverOptions<Integer, String> options = receiverOptions.subscription(Collections.singleton(topic))
//                .addAssignListener(partitions -> log.debug("onPartitionsAssigned {}", partitions))
//                .addRevokeListener(partitions -> log.debug("onPartitionsRevoked {}", partitions));
//        Flux<ReceiverRecord<Integer, String>> kafkaFlux = KafkaReceiver.create(options).receive();
//        return kafkaFlux.subscribe(record -> {
//            ReceiverOffset offset = record.receiverOffset();
//            Instant timestamp = Instant.ofEpochMilli(record.timestamp());
//            System.out.printf("Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
//                    offset.topicPartition(),
//                    offset.offset(),
//                    dateFormat.format(timestamp),
//                    record.key(),
//                    record.value());
//            offset.acknowledge();
//            latch.countDown();
//        });
//    }


    public static void main(String[] args) throws InterruptedException {
//        int count = 20;
//        CountDownLatch latch = new CountDownLatch(count);
//        ConsumerApplication consumer = new ConsumerApplication(BOOTSTRAP_SERVERS);
//        Disposable disposable = consumer.consumeMessages(TOPIC, latch);
//        latch.await(10, TimeUnit.SECONDS);
//        disposable.dispose();
        SpringApplication.run(ConsumerApplication.class, args);
    }

}
