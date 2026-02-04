package com.kafkalite.client.consumer;

import com.kafkalite.client.NetworkClient;
import com.kafkalite.model.Message;
import com.kafkalite.server.request.FetchRequest;
import com.kafkalite.server.response.FetchResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * 消息拉取器
 * 职责：处理Fetch请求、响应解析、Offset策略执行、长轮询实现
 */
@Slf4j
public class Fetcher {
    private final NetworkClient client;
    private final OffsetManager offsetManager;
    private final OffsetResetStrategy resetStrategy;
    private final int fetchMaxWaitMs;      // 长轮询最大等待时间
    //private final int fetchMinBytes;     //最小返回字节数（简易实现可忽略）
    private final int maxPollRecords;      // 单次最大返回记录数

    public Fetcher(NetworkClient client,
                   OffsetManager offsetManager,
                   OffsetResetStrategy resetStrategy,
                   int fetchMaxWaitMs,
                   int maxPollRecords) {
        this.client = client;
        this.offsetManager = offsetManager;
        this.resetStrategy = resetStrategy;
        this.fetchMaxWaitMs = fetchMaxWaitMs;
        this.maxPollRecords = maxPollRecords;
    }

    /**
     * 长轮询拉取消息（核心方法）
     * @param partitions 要拉取的分区集合
     * @param timeoutMs 最长等待时间
     * @return 拉取到的消息
     */
    public Map<TopicPartition, List<ConsumerRecord>> fetch(
            Set<TopicPartition> partitions,
            long timeoutMs) throws Exception {

        long deadline = System.currentTimeMillis() + timeoutMs;
        Map<TopicPartition, List<ConsumerRecord>> result = new HashMap<>();

        // 1. 检查并初始化Offset（针对新订阅的分区）
        for (TopicPartition tp : partitions) {
            if (offsetManager.nextOffset(tp) == 0 && resetStrategy == OffsetResetStrategy.LATEST) {
                // 需要获取HW（high watermark），这里简化处理，先发送一次fetch获取end offset
                long hw = fetchHighWatermark(tp);
                offsetManager.seekToEnd(tp, hw);
            }
        }

        // 2. 长轮询循环
        while (System.currentTimeMillis() < deadline && result.isEmpty()) {
            for (TopicPartition tp : partitions) {
                long offset = offsetManager.nextOffset(tp);
                List<ConsumerRecord> records = fetchOnce(tp, offset);

                if (!records.isEmpty()) {
                    result.put(tp, records);
                    // 更新消费位移
                    records.forEach(r -> offsetManager.updateConsumedOffset(tp, r.getOffset()));

                    if (result.values().stream().mapToInt(List::size).sum() >= maxPollRecords) {
                        break;
                    }
                }
            }

            if (result.isEmpty()) {
                // 无数据时短暂休眠避免CPU空转（简易版长轮询）
                Thread.sleep(Math.min(100, deadline - System.currentTimeMillis()));
            }
        }

        return result;
    }

    /**
     * 单次Fetch请求
     */
    private List<ConsumerRecord> fetchOnce(TopicPartition tp, long offset) {
        FetchRequest request = new FetchRequest();
        request.setTopic(tp.getTopic());
        request.setPartition(tp.getPartition());
        request.setOffset(offset);
        request.setMaxBytes(1024 * 1024); // 1MB

        CompletableFuture<FetchResponse> future = client.send(request)
                .thenApply(r -> (FetchResponse) r)
                .orTimeout(5, TimeUnit.SECONDS);

        try {
            FetchResponse response = future.get();

            if (response.getErrorCode() != 0) {
                log.error("Fetch error for {}: {}", tp, response.getErrorCode());
                return Collections.emptyList();
            }

            List<Message> messages = response.getMessageSet().getMessages();

            AtomicLong actualOffset = new AtomicLong(offset);

            return messages.stream()
                    .map(msg -> ConsumerRecord.fromMessage(
                            tp.getTopic(),
                            tp.getPartition(),
                            actualOffset.getAndIncrement(),
                            msg))
                    .limit(maxPollRecords)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Fetch failed for {}", tp, e);
            return Collections.emptyList();
        }
    }

    /**
     * 获取分区的高水位（简化实现：发送一个fetch获取logEndOffset）
     */
    long fetchHighWatermark(TopicPartition tp) throws Exception {
        // 实际应通过MetadataRequest获取，这里简化：发送fetch请求offset 0，取返回的highWatermark
        FetchRequest request = new FetchRequest();
        request.setTopic(tp.getTopic());
        request.setPartition(tp.getPartition());
        request.setOffset(0);
        request.setMaxBytes(1);

        FetchResponse response = (FetchResponse) client.send(request).get();
        return response.getHighWatermark();
    }
}