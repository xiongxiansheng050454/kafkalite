package com.kafkalite.client.producer;

import com.kafkalite.model.Message;
import com.kafkalite.model.MessageSet;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class RecordAccumulator {
    private final int batchSize;
    private final long lingerMs;
    private final Map<TopicPartition, Deque<RecordBatch>> batches;
    private final ReentrantLock lock;
    private final Condition notEmpty;
    private volatile boolean closed = false;

    // 记录待完成的Future，便于Sender查找
    private final Map<RecordBatch, CompletableFuture<KafkaProducer.RecordMetadata>> pendingFutures = new ConcurrentHashMap<>();

    public RecordAccumulator(int batchSize, long lingerMs) {
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.batches = new ConcurrentHashMap<>();
        this.lock = new ReentrantLock();
        this.notEmpty = lock.newCondition();
    }

    /**
     * 追加消息，返回关联的Future
     */
    public CompletableFuture<KafkaProducer.RecordMetadata> append(String topic, int partition, byte[] key, byte[] value) {
        TopicPartition tp = new TopicPartition(topic, partition);
        Message msg = new Message(key, value);

        int msgSize = estimateSize(key, value);
        Deque<RecordBatch> deque = batches.computeIfAbsent(tp, k -> new ConcurrentLinkedDeque<>());

        synchronized (deque) {
            RecordBatch last = deque.peekLast();

            // 创建新批次
            if (last == null || !last.hasRoom(msgSize)) {
                last = new RecordBatch(tp, batchSize, lingerMs);
                deque.addLast(last);

                lock.lock();
                try {
                    notEmpty.signalAll();
                } finally {
                    lock.unlock();
                }
            }

            last.append(msg);

            // 获取或创建该批次关联的Future

            return pendingFutures.computeIfAbsent(last,
                    k -> new CompletableFuture<>());
        }
    }

    /**
     * 获取就绪批次，并移除对应的Future映射（Sender取走后由Sender负责完成）
     */
    public List<BatchWithFuture> ready() {
        List<BatchWithFuture> ready = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();

            synchronized (deque) {
                RecordBatch batch = deque.peekFirst();
                if (batch != null && (batch.isFull() || batch.isExpired(now) || closed)) {
                    deque.pollFirst();
                    CompletableFuture<KafkaProducer.RecordMetadata> future = pendingFutures.remove(batch);
                    // 只有成功取出 future 的才加入待发送列表
                    if (future != null) {
                        ready.add(new BatchWithFuture(batch, future));
                    }
                }
            }
        }
        return ready;
    }

    /**
     * 强制关闭时，失败所有待处理的Future
     */
    public void close() {
        closed = true;
        lock.lock();
        try {
            notEmpty.signalAll();
        } finally {
            lock.unlock();
        }

        // 失败所有未发送的Future
        pendingFutures.forEach((batch, future) -> {
            if (!future.isDone()) {
                future.completeExceptionally(new RuntimeException("Producer closed"));
            }
        });
        pendingFutures.clear();
    }

    private int estimateSize(byte[] key, byte[] value) {
        return 4 + 8 + 4 + (key != null ? key.length : 0) + 4 + (value != null ? value.length : 0) + 8;
    }

    /**
     * 关键修复：直接检查队列和future是否为空，不使用泄漏的计数器
     */
    public boolean hasPending() {
        // 检查是否还有未取走的批次，或者还有未完成的 Future
        if (!pendingFutures.isEmpty()) {
            return true;
        }

        // 检查各分区队列是否为空
        for (Deque<RecordBatch> deque : batches.values()) {
            synchronized (deque) {
                if (!deque.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    public void awaitNotEmpty(long timeoutMs) throws InterruptedException {
        lock.lock();
        try {
            if (!hasPending()) {
                notEmpty.awaitNanos(timeoutMs * 1_000_000);
            }
        } finally {
            lock.unlock();
        }
    }

    public record TopicPartition(String topic, int partition) {}
    public record BatchWithFuture(RecordBatch batch, CompletableFuture<KafkaProducer.RecordMetadata> future) {}

    /**
     * 记录批次 - 携带TopicPartition信息
     */
    public static class RecordBatch {
        private final TopicPartition topicPartition;
        private final List<Message> records = new ArrayList<>();
        private final int capacity;
        private final long createTime;
        private final long lingerMs;
        private int currentSizeInBytes = 0;
        private volatile boolean isSent = false;

        public RecordBatch(TopicPartition tp, int capacity, long lingerMs) {
            this.topicPartition = tp;
            this.capacity = capacity;
            this.lingerMs = lingerMs;
            this.createTime = System.currentTimeMillis();
        }

        public void append(Message msg) {
            records.add(msg);
            currentSizeInBytes += 4 + 8 + 4 + msg.getKey().length + 4 + msg.getValue().length + 8;
        }

        public boolean hasRoom(int msgSize) {
            return currentSizeInBytes + msgSize <= capacity;
        }

        public boolean isFull() {
            return currentSizeInBytes >= capacity * 0.8;
        }

        public boolean isExpired(long now) {
            return now - createTime >= lingerMs;
        }

        public MessageSet toMessageSet() {
            MessageSet set = new MessageSet();
            records.forEach(set::append);
            return set;
        }

        public void complete() { isSent = true; }
        public boolean isCompleted() { return isSent; }
        public int size() { return records.size(); }
        public TopicPartition topicPartition() { return topicPartition; }
    }
}