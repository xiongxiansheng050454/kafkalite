package com.kafkalite.server.response;

import com.kafkalite.server.protocol.ResponseType;
import io.netty.buffer.ByteBuf;
import lombok.Data;

@Data
public abstract class AbstractResponse {
    public static final short SUCCESS = 0;
    public static final short UNKNOWN_TOPIC = 1;
    public static final short OFFSET_OUT_OF_RANGE = 2;

    protected short errorCode = SUCCESS;
    protected long correlationId;

    // 新增：返回响应类型，用于编码时写入类型字节
    public abstract ResponseType getResponseType();

    // 新增：解码方法（客户端用）
    public abstract void decode(ByteBuf buf);

    public abstract void encode(ByteBuf buf);
}
