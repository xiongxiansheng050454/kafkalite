package com.kafkalite.client.consumer;

import com.kafkalite.client.NetworkClient;
import com.kafkalite.server.request.MetadataRequest;
import com.kafkalite.server.response.MetadataResponse;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * KafkaConsumer 核心实现（拉取模式）*
 * 使用示例：
 * <pre>
 * Properties props = new Properties();
 * props.put("bootstrap.servers", "localhost:9092");
 * props.put("group.id", "test-group");
 * props.put("auto.offset.reset", "earliest");
 *
 * try (KafkaConsumer consumer = new KafkaConsumer(props)) {
 *     consumer.subscribe(Arrays.asList("test-topic"));
 *
 *     while (running) {
 *         ConsumerRecords records = consumer.poll(Duration.ofMillis(1000));
 *         records.forEach(record -> {
 *             System.out.println(record.valueAsString());
 *         });
 *         consumer.commitSync(); // 手动提交
 *     }
 * }
 * </pre>
 */
@Slf4j
public class KafkaConsumer implements AutoCloseable {
    private final String clientId;
    private final String groupId;
    private final NetworkClient networkClient;
    private final OffsetManager offsetManager;
    private final Fetcher fetcher;
    private final SubscriptionState subscriptionState;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean subscribed = new AtomicBoolean(false);

    // 配置参数
    private final long pollTimeoutMs;
    private final boolean autoCommitEnabled;

    public KafkaConsumer(Properties props) throws InterruptedException {
        this.clientId = props.getProperty("client.id", "consumer-" + UUID.randomUUID().toString().substring(0, 8));
        this.groupId = props.getProperty("group.id", "default-group");

        String servers = props.getProperty("bootstrap.servers", "localhost:9092");
        String[] parts = servers.split(":");
        String host = parts[0];
        int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;

        // 连接Broker
        this.networkClient = new NetworkClient(host, port);
        this.networkClient.connect();

        // 初始化Offset管理器
        this.autoCommitEnabled = Boolean.parseBoolean(props.getProperty("enable.auto.commit", "true"));
        long autoCommitIntervalMs = Long.parseLong(props.getProperty("auto.commit.interval.ms", "5000"));
        this.offsetManager = new OffsetManager(autoCommitEnabled, autoCommitIntervalMs);

        // 解析Offset重置策略
        String resetStrategy = props.getProperty("auto.offset.reset", "latest");
        OffsetResetStrategy strategy = switch (resetStrategy) {
            case "earliest" -> OffsetResetStrategy.EARLIEST;
            case "none" -> OffsetResetStrategy.NONE;
            default -> OffsetResetStrategy.LATEST;
        };

        // 初始化Fetcher
        int fetchMaxWaitMs = Integer.parseInt(props.getProperty("fetch.max.wait.ms", "500"));
        int maxPollRecords = Integer.parseInt(props.getProperty("max.poll.records", "500"));
        this.fetcher = new Fetcher(networkClient, offsetManager, strategy, fetchMaxWaitMs, maxPollRecords);

        this.subscriptionState = new SubscriptionState();
        this.pollTimeoutMs = Long.parseLong(props.getProperty("poll.timeout.ms", "300000")); // 默认5分钟
    }

    /**
     * 订阅指定Topic列表
     */
    public void subscribe(Collection<String> topics) {
        if (closed.get()) throw new IllegalStateException("Consumer is closed");

        // 对每个topic发送Metadata请求，确保topic被创建
        for (String topic : topics) {
            try {
                MetadataRequest request = new MetadataRequest();
                request.setTopic(topic);
                MetadataResponse response = (MetadataResponse) networkClient.send(request).get(5, TimeUnit.SECONDS);

                if (response.getTopicMetadata().containsKey(topic)) {
                    log.info("Topic {} metadata fetched, partitions: {}",
                            topic, response.getTopicMetadata().get(topic));
                }
            } catch (Exception e) {
                log.warn("Failed to fetch metadata for {}: {}", topic, e.getMessage());
            }
        }

        subscriptionState.subscribe(new HashSet<>(topics));
        subscribed.set(true);
        log.info("Subscribed to topics: {}", topics);
    }

    /**
     * 长轮询拉取消息（核心API）
     * @param timeout 最大等待时间
     * @return ConsumerRecords 消息集合（可能为空）
     */
    public ConsumerRecords poll(Duration timeout) {
        ensureOpen();
        if (!subscribed.get()) {
            throw new IllegalStateException("Consumer is not subscribed to any topics");
        }

        try {
            // 构建要拉取的分区集合（简化版：每个topic默认单分区partition 0）
            // Day 5后扩展：从Metadata获取实际分区数
            Set<TopicPartition> partitions = new HashSet<>();
            for (String topic : subscriptionState.subscription()) {
                // 简化：假设每个topic只有partition 0
                TopicPartition tp = new TopicPartition(topic, 0);
                partitions.add(tp);

                // 初始化offset（如果是新分区）
                if (offsetManager.nextOffset(tp) == 0) {
                    offsetManager.initializeOffset(tp, 0);
                }
            }

            // 执行长轮询
            long timeoutMs = Math.min(timeout.toMillis(), pollTimeoutMs);
            var records = fetcher.fetch(partitions, timeoutMs);

            return new ConsumerRecords(records);

        } catch (Exception e) {
            log.error("Poll failed", e);
            return ConsumerRecords.empty();
        }
    }

    /**
     * 手动提交所有分区位移
     */
    public void commitSync() {
        ensureOpen();
        Set<TopicPartition> tps = new HashSet<>();
        for (String topic : subscriptionState.subscription()) {
            tps.add(new TopicPartition(topic, 0));
        }
        offsetManager.commitSync(tps);
        log.debug("Committed offsets for group {}", groupId);
    }

    /**
     * 指定具体分区消费（高级API）
     */
    public void assign(Collection<TopicPartition> partitions) {
        if (closed.get()) throw new IllegalStateException("Consumer is closed");
        subscriptionState.assign(new HashSet<>(partitions));
        subscribed.set(true);

        // 初始化位移
        for (TopicPartition tp : partitions) {
            offsetManager.initializeOffset(tp, 0);
        }
        log.info("Assigned to partitions: {}", partitions);
    }

    /**
     * Seek操作：手动设置消费位置
     */
    public void seek(TopicPartition partition, long offset) {
        ensureOpen();
        offsetManager.seek(partition, offset);
    }

    public void seekToBeginning(Collection<TopicPartition> partitions) {
        partitions.forEach(offsetManager::seekToBeginning);
    }

    public void seekToEnd(Collection<TopicPartition> partitions) {
        // 需要获取每个分区的HW，这里简化处理
        partitions.forEach(tp -> {
            try {
                long hw = fetcher.fetchHighWatermark(tp);
                offsetManager.seekToEnd(tp, hw);
            } catch (Exception e) {
                log.error("Failed to seek to end for {}", tp, e);
            }
        });
    }

    /**
     * 获取当前消费位置
     */
    public long position(TopicPartition partition) {
        return offsetManager.nextOffset(partition);
    }

    /**
     * 获取已提交位移
     */
    public long committed(TopicPartition partition) {
        return offsetManager.committed(partition);
    }

    /**
     * 取消订阅
     */
    public void unsubscribe() {
        subscriptionState.unsubscribe();
        subscribed.set(false);
    }

    /**
     * 获取当前订阅
     */
    public Set<String> subscription() {
        return subscriptionState.subscription();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            log.info("Closing consumer {}", clientId);
            offsetManager.close();
            networkClient.close();
        }
    }

    private void ensureOpen() {
        if (closed.get()) throw new IllegalStateException("Consumer is closed");
    }

    /**
     * 订阅状态管理（内部类）
     */
    private static class SubscriptionState {
        private Set<String> subscription = new HashSet<>();
        private Set<TopicPartition> assignment = new HashSet<>();
        private boolean usesSubscription = true;

        void subscribe(Set<String> topics) {
            this.subscription = topics;
            this.usesSubscription = true;
        }

        void assign(Set<TopicPartition> partitions) {
            this.assignment = partitions;
            this.usesSubscription = false;
        }

        void unsubscribe() {
            subscription.clear();
            assignment.clear();
        }

        Set<String> subscription() {
            return usesSubscription ? subscription :
                    assignment.stream().map(TopicPartition::getTopic).collect(Collectors.toSet());
        }
    }
}