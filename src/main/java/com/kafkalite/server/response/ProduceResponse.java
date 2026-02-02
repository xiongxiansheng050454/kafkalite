package com.kafkalite.server.response;

import com.kafkalite.server.protocol.ResponseType;
import io.netty.buffer.ByteBuf;
import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class ProduceResponse extends AbstractResponse{
    private long offset; // 返回写入的起始 offset

    @Override
    public ResponseType getResponseType() {
        return ResponseType.PRODUCE;
    }

    @Override
    public void encode(ByteBuf buf) {
        buf.writeShort(errorCode);
        buf.writeLong(offset);
    }

    @Override
    public void decode(ByteBuf buf) {
        this.errorCode = buf.readShort();
        this.offset = buf.readLong();
    }
}
