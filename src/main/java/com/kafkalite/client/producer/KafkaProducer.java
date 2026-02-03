package com.kafkalite.client.producer;

import com.kafkalite.client.NetworkClient;
import com.kafkalite.client.metadata.MetadataManager;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.concurrent.*;

@Slf4j
public class KafkaProducer {
    private final ProducerConfig config;
    private final Partitioner partitioner;
    private final RecordAccumulator accumulator;
    private final NetworkClient client;
    private final Sender sender;
    private final MetadataManager metadataManager;
    private final ExecutorService callbackExecutor;

    public KafkaProducer(Properties props) throws InterruptedException {
        this.config = new ProducerConfig(props);
        this.partitioner = createPartitioner(config.partitionStrategy);
        this.accumulator = new RecordAccumulator(config.batchSize, config.lingerMs);
        this.client = new NetworkClient(config.host, config.port);
        this.client.connect();

        // 初始化元数据管理器（在Sender之前）
        this.metadataManager = new MetadataManager(client, 30000, 60000); // 30s刷新，60s过期

        this.sender = new Sender(accumulator, client, config.retries, config.retryBackoffMs);
        this.callbackExecutor = Executors.newFixedThreadPool(4);

        this.sender.start();
    }

    /**
     * 异步发送 - 动态获取分区数
     */
    public CompletableFuture<RecordMetadata> sendAsync(String topic, byte[] key, byte[] value) {
        // 1. 尝试获取分区数
        int partitionCount = metadataManager.getPartitionCount(topic);

        if (partitionCount <= 0) {
            // 2. 未知Topic，触发元数据刷新
            return metadataManager.refresh(topic)
                    .thenCompose(meta -> doSend(topic, key, value, meta.getPartitionCount()))
                    .exceptionally(e -> {
                        log.error("Failed to get metadata for {}", topic, e);
                        throw new CompletionException(e);
                    });
        }

        // 3. 已知Topic，直接发送
        return doSend(topic, key, value, partitionCount);
    }

    /**
     * 实际发送逻辑
     */
    private CompletableFuture<RecordMetadata> doSend(String topic, byte[] key, byte[] value, int partitionCount) {
        // 4. 计算分区（使用实际分区数）
        int partition = partitioner.partition(null, key, value, partitionCount);

        // 5. 追加到累加器
        return accumulator.append(topic, partition, key, value);
    }

    public RecordMetadata send(String topic, byte[] key, byte[] value)
            throws InterruptedException, ExecutionException, TimeoutException {
        return sendAsync(topic, key, value)
                .get(config.requestTimeoutMs, TimeUnit.MILLISECONDS);
    }

    public void send(String topic, byte[] key, byte[] value, Callback callback) {
        sendAsync(topic, key, value).whenCompleteAsync((metadata, exception) -> {
            if (callback != null) {
                callback.onCompletion(metadata, (Exception) exception);
            }
        }, callbackExecutor);
    }

    public void flushWithTimeout(long timeoutMs) {
        log.info("Flushing (max {}ms)...", timeoutMs);
        long start = System.currentTimeMillis();
        while (accumulator.hasPending()) {
            if (System.currentTimeMillis() - start > timeoutMs) {
                log.warn("Flush timeout after {}ms, forcing proceed", timeoutMs);
                break;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        // 额外等待确保Sender处理完
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void close() {
        log.info("Closing producer...");
        // 1. 先标记 accumulator 关闭，阻止新消息进入
        accumulator.close();
        // 2. 等待 Sender 处理完剩余数据（带超时）
        flushWithTimeout(10000); // 最多等待10秒
        sender.shutdown();
        metadataManager.close(); // 关闭元数据管理器
        callbackExecutor.shutdown();
        try {
            if (!callbackExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                callbackExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            callbackExecutor.shutdownNow();
        }
        // 5. 关闭网络连接
        client.close();
        log.info("Producer closed");
    }

    private Partitioner createPartitioner(String strategy) {
        return switch (strategy.toLowerCase()) {
            case "roundrobin" -> new RoundRobinPartitioner();
            case "random" -> new RandomPartitioner();
            default -> new HashPartitioner();
        };
    }

    @FunctionalInterface
    public interface Callback {
        void onCompletion(RecordMetadata metadata, Exception exception);
    }

    public record RecordMetadata(String topic, int partition, long offset, long timestamp) {}

    @Data
    public static class ProducerConfig {
        private String host;
        private int port;
        private String partitionStrategy;
        private int batchSize;
        private long lingerMs;
        private int retries;
        private long retryBackoffMs;
        private int requestTimeoutMs;

        public ProducerConfig(Properties props) {
            String servers = props.getProperty("bootstrap.servers", "localhost:9092");
            String[] parts = servers.split(":");
            this.host = parts[0];
            this.port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;
            this.partitionStrategy = props.getProperty("partitioner.class", "hash");
            this.batchSize = Integer.parseInt(props.getProperty("batch.size", "16384"));
            this.lingerMs = Long.parseLong(props.getProperty("linger.ms", "100"));
            this.retries = Integer.parseInt(props.getProperty("retries", "3"));
            this.retryBackoffMs = Long.parseLong(props.getProperty("retry.backoff.ms", "100"));
            this.requestTimeoutMs = Integer.parseInt(props.getProperty("request.timeout.ms", "30000"));
        }
    }
}