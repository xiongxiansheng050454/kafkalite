package com.kafkalite.client.metadata;

import com.kafkalite.client.NetworkClient;
import com.kafkalite.server.request.MetadataRequest;
import com.kafkalite.server.response.MetadataResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 元数据管理器：管理 Topic 到 Partition 的映射，支持自动刷新
 */
@Slf4j
public class MetadataManager {
    // 本地缓存
    private final Map<String, TopicMetadata> cache = new ConcurrentHashMap<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    // 刷新控制
    private final long refreshIntervalMs;
    private final long metadataMaxAgeMs;
    private final NetworkClient client;
    private final ScheduledExecutorService scheduler;
    private volatile boolean closed = false;

    // 正在刷新的 Topic（防止并发刷新）
    private final Set<String> pendingRefresh = ConcurrentHashMap.newKeySet();

    public MetadataManager(NetworkClient client, long refreshIntervalMs, long metadataMaxAgeMs) {
        this.client = client;
        this.refreshIntervalMs = refreshIntervalMs;
        this.metadataMaxAgeMs = metadataMaxAgeMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kafka-metadata-refresher");
            t.setDaemon(true);
            return t;
        });

        // 启动定时刷新
        startPeriodicRefresh();
    }

    /**
     * 获取分区数（核心API，供Producer使用）
     * @return 分区数，如果未知返回 -1 触发刷新
     */
    public int getPartitionCount(String topic) {
        lock.readLock().lock();
        try {
            TopicMetadata metadata = cache.get(topic);
            if (metadata != null && !isExpired(metadata)) {
                return metadata.getPartitionCount();
            }
            return -1; // 需要刷新
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 强制刷新指定 Topic（首次发送或过期时调用）
     */
    public CompletableFuture<TopicMetadata> refresh(String topic) {
        if (pendingRefresh.contains(topic)) {
            // 已有刷新在进行，等待完成
            return waitForRefresh(topic);
        }

        pendingRefresh.add(topic);

        return CompletableFuture.supplyAsync(() -> {
            try {
                log.info("Refreshing metadata for topic: {}", topic);

                MetadataRequest request = new MetadataRequest();
                request.setTopic(topic);

                MetadataResponse response = (MetadataResponse) client.send(request).get();

                if (response.getErrorCode() != 0) {
                    throw new RuntimeException("Metadata fetch failed: " + response.getErrorCode());
                }

                Integer partitionCount = response.getTopicMetadata().get(topic);
                if (partitionCount == null) {
                    // Topic不存在，创建默认单分区（自动创建Topic场景）
                    partitionCount = 1;
                    log.warn("Topic {} not found on broker, using default partition count 1", topic);
                }

                TopicMetadata metadata = new TopicMetadata(topic, partitionCount,
                        (int) System.currentTimeMillis()); // 简单epoch

                lock.writeLock().lock();
                try {
                    cache.put(topic, metadata);
                } finally {
                    lock.writeLock().unlock();
                }

                log.info("Updated metadata for {}: {} partitions", topic, partitionCount);
                return metadata;

            } catch (Exception e) {
                log.error("Failed to refresh metadata for {}", topic, e);
                throw new CompletionException(e);
            } finally {
                pendingRefresh.remove(topic);
            }
        });
    }

    /**
     * 刷新所有 Topic（定期任务）
     */
    public void refreshAll() {
        Set<String> topics;
        lock.readLock().lock();
        try {
            topics = new HashSet<>(cache.keySet());
        } finally {
            lock.readLock().unlock();
        }

        topics.forEach(this::refresh);
    }

    /**
     * 阻塞等待直到获取到分区信息（带超时）
     */
    public int awaitPartitionCount(String topic, long timeoutMs) throws InterruptedException, ExecutionException, TimeoutException {
        int count = getPartitionCount(topic);
        if (count > 0) return count;

        // 触发刷新并等待
        return refresh(topic).thenApply(TopicMetadata::getPartitionCount)
                .get(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private boolean isExpired(TopicMetadata metadata) {
        return System.currentTimeMillis() - metadata.getUpdateTimeMs() > metadataMaxAgeMs;
    }

    private CompletableFuture<TopicMetadata> waitForRefresh(String topic) {
        return CompletableFuture.supplyAsync(() -> {
            long start = System.currentTimeMillis();
            while (pendingRefresh.contains(topic)) {
                if (System.currentTimeMillis() - start > 5000) {
                    throw new RuntimeException("Wait for refresh timeout");
                }
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
            return cache.get(topic);
        });
    }

    private void startPeriodicRefresh() {
        scheduler.scheduleAtFixedRate(() -> {
            if (!closed) {
                refreshAll();
            }
        }, refreshIntervalMs, refreshIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void close() {
        closed = true;
        scheduler.shutdown();
    }
}