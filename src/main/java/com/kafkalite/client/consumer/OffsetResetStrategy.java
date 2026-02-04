package com.kafkalite.client.consumer;

/**
 * Offset重置策略
 */
public enum OffsetResetStrategy {
    EARLIEST,   // 从最早开始
    LATEST,     // 从最新开始（默认）
    NONE        // 不重置，报错
}
