package com.kafkalite;

import com.kafkalite.client.consumer.ConsumerRecord;
import com.kafkalite.client.consumer.ConsumerRecords;
import com.kafkalite.client.consumer.KafkaConsumer;
import com.kafkalite.client.producer.KafkaProducer;
import com.kafkalite.client.consumer.TopicPartition;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 完整的Producer-Consumer集成测试
 * 流程：Producer发送 -> Consumer拉取 -> 验证内容
 */
@Slf4j
public class ProducerConsumerIntegrationTest {

    public static void main(String[] args) throws Exception {
        String topic = "integration-test-topic";
        String bootstrapServers = "localhost:9092";

        // ========== 步骤1：Producer发送消息 ==========
        log.info("========== Phase 1: Producing Messages ==========");

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", bootstrapServers);
        producerProps.put("batch.size", "1"); // 测试时立即发送，不等待批量
        producerProps.put("linger.ms", "0");  // 无延迟

        KafkaProducer producer = new KafkaProducer(producerProps);

        // 发送多条消息，等待每条确认
        int messageCount = 5;
        for (int i = 0; i < messageCount; i++) {
            String key = "key-" + i;
            String value = "message-content-" + i;

            // 同步发送并获取元数据
            var metadata = producer.send(topic, key.getBytes(), value.getBytes());
            log.info("Produced message {}: topic={}, partition={}, offset={}",
                    i, metadata.topic(), metadata.partition(), metadata.offset());
        }

        // 确保所有消息都发送到Broker
        producer.flushWithTimeout(5000);
        log.info("Total {} messages produced successfully", messageCount);
        producer.close();

        // 短暂停顿确保Broker处理完成（生产环境不需要）
        Thread.sleep(500);

        // ========== 步骤2：Consumer拉取消息 ==========
        log.info("========== Phase 2: Consuming Messages ==========");

        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", bootstrapServers);
        consumerProps.put("group.id", "integration-test-group");
        consumerProps.put("client.id", "integration-test-client");
        consumerProps.put("auto.offset.reset", "earliest"); // 关键：从最早开始
        consumerProps.put("enable.auto.commit", "false");   // 手动提交便于观察

        try (KafkaConsumer consumer = new KafkaConsumer(consumerProps)) {
            // 订阅Topic（会在内部发送Metadata请求确保Topic存在）
            consumer.subscribe(Arrays.asList(topic));

            // 第一次Poll：应该能拉取到所有消息
            log.info("Polling for messages...");
            ConsumerRecords records = consumer.poll(Duration.ofSeconds(5));

            log.info("Fetched {} records", records.count());

            // 验证消息内容
            int receivedCount = 0;
            for (ConsumerRecord record : records) {
                log.info("Received: partition={}, offset={}, key={}, value={}",
                        record.getPartition(),
                        record.getOffset(),
                        new String(record.getKey()),
                        record.valueAsString());
                receivedCount++;
            }

            // 断言验证
            if (receivedCount != messageCount) {
                log.error("Message count mismatch! Expected: {}, Actual: {}",
                        messageCount, receivedCount);
            } else {
                log.info("✓ All messages consumed successfully");
            }

            // 手动提交位移（如果不提交，下次重启Consumer会重复消费）
            if (receivedCount > 0) {
                consumer.commitSync();
                log.info("Offsets committed");
            }

            // ========== 步骤3：验证Seek功能 ==========
            log.info("========== Phase 3: Testing Seek ==========");

            // Seek到第2条消息（offset=2）重新消费
            TopicPartition tp = new TopicPartition(topic, 0);
            consumer.seek(tp, 2);
            log.info("Seeked to offset 2, current position: {}",
                    consumer.position(tp));

            ConsumerRecords reFetched = consumer.poll(Duration.ofSeconds(3));
            log.info("Re-fetched {} records from offset 2", reFetched.count());

            reFetched.forEach(record -> {
                log.info("Re-read: offset={}, value={}",
                        record.getOffset(), record.valueAsString());
            });

            // 测试从Latest消费（应该为空，因为已经消费到末尾）
            consumer.seekToEnd(Arrays.asList(tp));
            log.info("Seeked to end, position: {}", consumer.position(tp));

            ConsumerRecords endRecords = consumer.poll(Duration.ofSeconds(2));
            log.info("After seeking to end, fetched {} records (should be 0)",
                    endRecords.count());
        }

        log.info("========== Test Completed ==========");
    }
}