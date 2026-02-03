package com.kafkalite.server.request;

import com.kafkalite.server.protocol.RequestType;
import io.netty.buffer.ByteBuf;
import lombok.Data;

@Data
public abstract class AbstractRequest {
    protected String topic;
    protected int partition;
    protected long correlationId;

    public abstract RequestType getRequestType();

    // 从 ByteBuf 解码具体字段
    public abstract void decode(ByteBuf buf);

    // 编码到 ByteBuf（Client 端使用）
    public abstract void encode(ByteBuf buf);
}
