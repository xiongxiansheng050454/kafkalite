package com.kafkalite.queue;

import com.kafkalite.model.Message;
import com.kafkalite.model.MessageSet;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryQueue {
    private final ArrayDeque<ByteBuffer> queue;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    private volatile long logEndOffset = 0; // 下一条要写入的offset

    public MemoryQueue() {
        this.queue = new ArrayDeque<>();
    }

    /**
     * 追加消息（Producer 调用）
     */
    public void append(MessageSet messageSet) {
        writeLock.lock();
        try {
            ByteBuffer buffer = messageSet.toByteBuffer();
            // 为每个消息分配offset（简化版，实际应在MessageSet级别分配）
            for (int i = 0; i < messageSet.size(); i++) {
                logEndOffset++;
            }
            queue.addLast(buffer);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * 读取消息（Consumer 调用）
     * @param startOffset 起始偏移量（Day1简化：忽略，从队列头读）
     * @param maxBytes 最大读取字节数
     */
    public List<Message> read(long startOffset, int maxBytes) {
        readLock.lock();
        try {
            List<Message> result = new ArrayList<>();
            int bytesRead = 0;

            for (ByteBuffer buffer : queue) {
                if (bytesRead + buffer.remaining() > maxBytes) break;

                ByteBuffer duplicate = buffer.duplicate(); // 避免修改原buffer
                result.addAll(MessageSet.fromByteBuffer(duplicate));
                bytesRead += buffer.remaining();
            }
            return result;
        } finally {
            readLock.unlock();
        }
    }

    public long getLogEndOffset() {
        readLock.lock();
        try {
            return logEndOffset;
        } finally {
            readLock.unlock();
        }
    }

    public int size() {
        readLock.lock();
        try {
            return queue.size();
        } finally {
            readLock.unlock();
        }
    }
}
