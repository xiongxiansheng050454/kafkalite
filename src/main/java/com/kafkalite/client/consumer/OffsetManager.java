package com.kafkalite.client.consumer;

import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 位移管理器（内存版）
 * 职责：管理每个TopicPartition的消费位移，支持自动/手动提交
 */
@Slf4j
public class OffsetManager {
    // 当前消费位移（下一条要拉取的offset）
    private final ConcurrentMap<TopicPartition, Long> consumedOffsets = new ConcurrentHashMap<>();
    // 已提交位移
    private final ConcurrentMap<TopicPartition, Long> committedOffsets = new ConcurrentHashMap<>();

    private final boolean autoCommitEnabled;
    private final long autoCommitIntervalMs;
    private final ScheduledExecutorService autoCommitScheduler;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public OffsetManager(boolean autoCommitEnabled, long autoCommitIntervalMs) {
        this.autoCommitEnabled = autoCommitEnabled;
        this.autoCommitIntervalMs = autoCommitIntervalMs;

        if (autoCommitEnabled) {
            this.autoCommitScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "kafka-consumer-auto-commit");
                t.setDaemon(true);
                return t;
            });
            startAutoCommit();
        } else {
            this.autoCommitScheduler = null;
        }
    }

    /**
     * 设置初始位移（订阅时调用）
     */
    public void initializeOffset(TopicPartition tp, long offset) {
        consumedOffsets.putIfAbsent(tp, offset);
        committedOffsets.putIfAbsent(tp, offset);
    }

    /**
     * 获取下一个要拉取的offset
     */
    public long nextOffset(TopicPartition tp) {
        return consumedOffsets.getOrDefault(tp, 0L);
    }

    /**
     * 更新消费位移（poll到新消息后调用）
     */
    public void updateConsumedOffset(TopicPartition tp, long offset) {
        consumedOffsets.put(tp, offset + 1); // 指向下一条
    }

    /**
     * 手动提交指定分区的位移
     */
    public void commitSync(TopicPartition tp) {
        Long offset = consumedOffsets.get(tp);
        if (offset != null) {
            committedOffsets.put(tp, offset);
            log.debug("Committed offset {} for {}", offset, tp);
        }
    }

    public void commitSync(Set<TopicPartition> partitions) {
        partitions.forEach(this::commitSync);
    }

    /**
     * 手动指定位移（seek操作）
     */
    public void seek(TopicPartition tp, long offset) {
        consumedOffsets.put(tp, offset);
        log.info("Seeked {} to offset {}", tp, offset);
    }

    public void seekToBeginning(TopicPartition tp) {
        seek(tp, 0L);
    }

    public void seekToEnd(TopicPartition tp, long highWatermark) {
        seek(tp, highWatermark);
    }

    /**
     * 获取已提交位移
     */
    public long committed(TopicPartition tp) {
        return committedOffsets.getOrDefault(tp, -1L);
    }

    private void startAutoCommit() {
        autoCommitScheduler.scheduleAtFixedRate(() -> {
            if (!closed.get()) {
                commitAll();
            }
        }, autoCommitIntervalMs, autoCommitIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void commitAll() {
        consumedOffsets.forEach((tp, offset) -> {
            committedOffsets.put(tp, offset);
            log.debug("Auto-committed offset {} for {}", offset, tp);
        });
    }

    public void close() {
        if (closed.compareAndSet(false, true) && autoCommitScheduler != null) {
            commitAll(); // 最后一次提交
            autoCommitScheduler.shutdown();
            try {
                if (!autoCommitScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    autoCommitScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                autoCommitScheduler.shutdownNow();
            }
        }
    }
}