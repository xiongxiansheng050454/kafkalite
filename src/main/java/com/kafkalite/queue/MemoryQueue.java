package com.kafkalite.queue;

import com.kafkalite.model.Message;
import com.kafkalite.model.MessageSet;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemoryQueue {
    private final ArrayDeque<ByteBuffer> queue;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock readLock = lock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    // 无需 volatile，由 writeLock/readLock 保证可见性
    private long logEndOffset = 0; // 下一条要写入的offset

    public MemoryQueue() {
        this.queue = new ArrayDeque<>();
    }

    /**
     * 追加消息（Producer 调用）
     */
    public void append(MessageSet messageSet) {
        writeLock.lock();
        try {
            // 为消息集中的每条消息分配正确的 partition offset
            List<Message> messages = messageSet.getMessages();
            for (Message msg : messages) {
                // 创建新消息，使用正确的 partition offset
                Message correctedMsg = new Message(msg.getKey(), msg.getValue(), logEndOffset);
                logEndOffset++;
            }

            ByteBuffer buffer = messageSet.toByteBuffer();
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
            long currentOffset = startOffset;

            // 跳过 startOffset 之前的消息（简化实现：假设每条消息占一个slot）
            Iterator<ByteBuffer> it = queue.iterator();
            for (long i = 0; i < startOffset && it.hasNext(); i++) {
                it.next();
            }

            // 从 startOffset 开始读取
            while (it.hasNext() && bytesRead < maxBytes) {
                ByteBuffer buffer = it.next();
                ByteBuffer duplicate = buffer.duplicate();

                // 反序列化并设置正确的 offset
                List<Message> messages = MessageSet.fromByteBuffer(duplicate, currentOffset);
                result.addAll(messages);

                currentOffset += messages.size();
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
