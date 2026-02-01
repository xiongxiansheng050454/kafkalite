package com.kafkalite.model;

import com.kafkalite.queue.MemoryQueue;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

public class Partition {
    private final int partitionId;
    private final String topicName;
    private final MemoryQueue log; // 核心：每个Partition对应一个Log（内存队列）
    private final AtomicInteger leaderEpoch = new AtomicInteger(0);

    public Partition(int partitionId, String topicName) {
        this.partitionId = partitionId;
        this.topicName = topicName;
        this.log = new MemoryQueue();
    }

    public void append(MessageSet messages) {
        log.append(messages);
    }

    public void append(Message message) {
        MessageSet set = new MessageSet();
        set.append(message);
        log.append(set);
    }

    public List<Message> fetch(long startOffset, int maxBytes) {
        return log.read(startOffset, maxBytes);
    }

    public long logEndOffset() {
        return log.getLogEndOffset();
    }

    public int getPartitionId() { return partitionId; }
}
