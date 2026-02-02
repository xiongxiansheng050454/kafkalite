package com.kafkalite;

import com.kafkalite.client.SimpleConsumer;
import com.kafkalite.client.SimpleProducer;
import com.kafkalite.server.KafkaBrokerServer;
import io.netty.util.ResourceLeakDetector;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class Day2IntegrationTest {

    static {
        // 开发阶段使用 PARANOID，生产使用 SIMPLE 或 DISABLED
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.PARANOID);
    }

    static ExecutorService executor = Executors.newSingleThreadExecutor();
    static final int PORT = 19092; // 避免与本地 Kafka 冲突

    @BeforeAll
    static void startServer() {
        executor.submit(() -> {
            try {
                new KafkaBrokerServer(PORT).start();
            } catch (InterruptedException e) {
                log.error("Server error", e);
            }
        });
        // 等待启动
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {}
    }

    @Test
    void testProduceAndConsume() throws Exception {
        SimpleProducer producer = new SimpleProducer("localhost", PORT);
        SimpleConsumer consumer = new SimpleConsumer("localhost", PORT);

        // 1. 发送消息
        String topic = "test-topic";
        long offset1 = producer.send(topic, 0, "key1", "Hello KafkaLite!");
        long offset2 = producer.send(topic, 0, "key2", "Second message");

        log.info("Produced messages at offset {} and {}", offset1, offset2);
        assert offset1 == 1 && offset2 == 2; // Day1 的 offset 从 1 开始计数

        // 2. 消费消息（从 offset 0 开始）
        var messages = consumer.poll(topic, 0, 0, 1024 * 1024);
        log.info("Fetched {} messages", messages.size());

        assert messages.size() == 2;
        assert "Hello KafkaLite!".equals(messages.get(0).getValueAsString());

        producer.close();
        consumer.close();
        log.info("Day2 test passed!");
    }

    @AfterAll
    static void shutdown() {
        executor.shutdownNow();
    }
}