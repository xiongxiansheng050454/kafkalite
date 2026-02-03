package com.kafkalite;

import com.kafkalite.client.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ProducerTest {

    @Test
    public void testAsyncProducer() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("batch.size", "1024");      // 小批次便于测试
        props.put("linger.ms", "1000");       // 1秒延迟，确保批量
        props.put("retries", "3");
        props.put("request.timeout.ms", "10000");
        props.put("partitioner.class", "hash");

        KafkaProducer producer = new KafkaProducer(props);

        int msgCount = 100;
        CountDownLatch latch = new CountDownLatch(msgCount);
        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failed = new AtomicInteger(0);

        long start = System.currentTimeMillis();

        for (int i = 0; i < msgCount; i++) {
            final int id = i;
            String key = "key-" + (i % 10);
            String value = "message-content-" + i;

            // 使用回调方式
            producer.send("test-topic", key.getBytes(), value.getBytes(),
                    (metadata, exception) -> {
                        if (exception == null) {
                            success.incrementAndGet();
                            if (id % 20 == 0) { // 减少日志输出
                                log.info("Sent[{}] to {}-{} offset={}",
                                        id, metadata.topic(), metadata.partition(), metadata.offset());
                            }
                        } else {
                            failed.incrementAndGet();
                            log.error("Failed[{}]: {}", id, exception.getMessage());
                        }
                        latch.countDown();
                    });
        }

        log.info("All messages queued, waiting for completion...");

        // 最多等待60秒
        boolean completed = latch.await(60, TimeUnit.SECONDS);
        long elapsed = System.currentTimeMillis() - start;

        log.info("Test completed in {}ms, latch completed: {}", elapsed, completed);
        log.info("Success: {}, Failed: {}", success.get(), failed.get());

        producer.close();

        assert success.get() + failed.get() == msgCount : "Not all callbacks invoked";
    }

    @Test
    public void testPartitioner() {
        // 直接测试分区策略
        var partitioner = new com.kafkalite.client.producer.HashPartitioner();
        var mockTopic = new com.kafkalite.model.Topic("test", 3);

        // 相同key应落到相同分区
        int p1 = partitioner.partition(mockTopic, "user123".getBytes(), null, 3);
        int p2 = partitioner.partition(mockTopic, "user123".getBytes(), null, 3);
        assert p1 == p2 : "Hash partitioner inconsistency";

        System.out.println("Partition for user123: " + p1);
    }

    @Test
    public void testDynamicPartitionDiscovery() throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");

        KafkaProducer producer = new KafkaProducer(props);

        // 首次发送：自动获取元数据（之前未知test-topic）
        // 如果Broker上test-topic有3个分区，消息会被正确路由到0/1/2
        for (int i = 0; i < 9; i++) {
            String key = "key-" + i; // 相同key应落到同一分区
            producer.send("test-topic", key.getBytes(), ("msg-" + i).getBytes());
        }

        producer.close();
    }
}