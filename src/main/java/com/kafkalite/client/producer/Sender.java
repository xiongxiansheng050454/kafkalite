package com.kafkalite.client.producer;

import com.kafkalite.client.NetworkClient;
import com.kafkalite.model.MessageSet;
import com.kafkalite.server.request.ProduceRequest;
import com.kafkalite.server.response.ProduceResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class Sender implements Runnable {
    private final RecordAccumulator accumulator;
    private final NetworkClient client;
    private final int retries;
    private final long retryBackoffMs;
    private final AtomicBoolean running;
    private final Thread senderThread;
    private final Map<RecordAccumulator.TopicPartition, Integer> retryCounts = new ConcurrentHashMap<>();
    private final ScheduledExecutorService retryScheduler = Executors.newScheduledThreadPool(1);

    public Sender(RecordAccumulator accumulator, NetworkClient client,
                  int retries, long retryBackoffMs) {
        this.accumulator = accumulator;
        this.client = client;
        this.retries = retries;
        this.retryBackoffMs = retryBackoffMs;
        this.running = new AtomicBoolean(true);
        this.senderThread = new Thread(this, "kafka-producer-network-thread");
    }

    public void start() {
        senderThread.start();
    }

    public void shutdown() {
        running.set(false);
        senderThread.interrupt();

        retryScheduler.shutdown();
        try {
            if (!retryScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                retryScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            retryScheduler.shutdownNow();
        }

        try {
            senderThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void run() {
        log.info("Sender thread started");

        while (running.get()) {
            try {
                List<RecordAccumulator.BatchWithFuture> batches = accumulator.ready();

                if (batches.isEmpty()) {
                    accumulator.awaitNotEmpty(100);
                    continue;
                }

                // 发送所有就绪批次
                for (RecordAccumulator.BatchWithFuture bwf : batches) {
                    sendBatch(bwf.batch(), bwf.future());
                }

            } catch (InterruptedException e) {
                if (!running.get()) break;
            } catch (Exception e) {
                log.error("Unexpected error in sender thread", e);
            }
        }

        flush();
        log.info("Sender thread terminated");
    }

    private void sendBatch(RecordAccumulator.RecordBatch batch,
                           CompletableFuture<KafkaProducer.RecordMetadata> future) {
        RecordAccumulator.TopicPartition tp = batch.topicPartition();

        // 已完成的批次不再发送
        if (batch.isCompleted()) {
            return;
        }

        try {
            MessageSet messageSet = batch.toMessageSet();
            ProduceRequest request = new ProduceRequest();
            request.setTopic(tp.topic());
            request.setPartition(tp.partition());
            request.setMessageSet(messageSet);

            log.debug("Sending batch to {}-{}, {} messages", tp.topic(), tp.partition(), batch.size());

            client.send(request).thenAccept(response -> {
                handleResponse(batch, future, (ProduceResponse) response, tp);
            }).orTimeout(5, TimeUnit.SECONDS).exceptionally(e -> {
                handleError(batch, future, e, tp);
                return null;
            });

        } catch (Exception e) {
            handleError(batch, future, e, tp);
        }
    }

    private void handleResponse(RecordAccumulator.RecordBatch batch,
                                CompletableFuture<KafkaProducer.RecordMetadata> future,
                                ProduceResponse response,
                                RecordAccumulator.TopicPartition tp) {
        if (response.getErrorCode() == 0) {
            batch.complete();
            retryCounts.remove(tp);

            // 关键：完成Future，通知调用方
            KafkaProducer.RecordMetadata metadata = new KafkaProducer.RecordMetadata(
                    tp.topic(),
                    tp.partition(),
                    response.getOffset(),
                    System.currentTimeMillis()
            );
            future.complete(metadata);

            log.debug("Batch sent successfully to {}-{}, offset: {}",
                    tp.topic(), tp.partition(), response.getOffset());
        } else {
            if (isRetriableError(response.getErrorCode())) {
                scheduleRetry(batch, future, tp, "Server error: " + response.getErrorCode());
            } else {
                batch.complete();
                future.completeExceptionally(
                        new RuntimeException("Non-retriable error: " + response.getErrorCode())
                );
            }
        }
    }

    private void handleError(RecordAccumulator.RecordBatch batch,
                             CompletableFuture<KafkaProducer.RecordMetadata> future,
                             Throwable e,
                             RecordAccumulator.TopicPartition tp) {
        log.warn("Send failed to {}-{}: {}", tp.topic(), tp.partition(), e.getMessage());

        if (e instanceof TimeoutException) {
            scheduleRetry(batch, future, tp, "Timeout");
        } else {
            batch.complete();
            future.completeExceptionally(e);
        }
    }

    private void scheduleRetry(RecordAccumulator.RecordBatch batch,
                               CompletableFuture<KafkaProducer.RecordMetadata> future,
                               RecordAccumulator.TopicPartition tp,
                               String reason) {
        int retryCount = retryCounts.getOrDefault(tp, 0);

        if (retryCount < retries) {
            retryCounts.put(tp, retryCount + 1);
            long backoff = retryBackoffMs * (1L << retryCount);

            log.info("Scheduling retry for {}-{} in {}ms, attempt {}/{}",
                    tp.topic(), tp.partition(), backoff, retryCount + 1, retries);

            retryScheduler.schedule(() -> {
                sendBatch(batch, future);
            }, backoff, TimeUnit.MILLISECONDS);
        } else {
            log.error("Batch failed after {} retries for {}-{}: {}",
                    retries, tp.topic(), tp.partition(), reason);
            batch.complete();
            future.completeExceptionally(
                    new RuntimeException("Failed after " + retries + " retries: " + reason)
            );
        }
    }

    private boolean isRetriableError(short errorCode) {
        return errorCode == 1 || errorCode == 2 || errorCode == 3; // 可扩展
    }

    private void flush() {
        log.info("Flushing remaining batches...");
        List<RecordAccumulator.BatchWithFuture> remaining;
        while (!(remaining = accumulator.ready()).isEmpty()) {
            remaining.forEach(bwf -> sendBatch(bwf.batch(), bwf.future()));
        }
        // 等待所有飞行中请求完成
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}