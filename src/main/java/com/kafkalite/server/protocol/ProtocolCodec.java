package com.kafkalite.server.protocol;

import com.kafkalite.server.request.AbstractRequest;
import com.kafkalite.server.request.FetchRequest;
import com.kafkalite.server.request.MetadataRequest;
import com.kafkalite.server.request.ProduceRequest;
import com.kafkalite.server.response.AbstractResponse;
import com.kafkalite.server.response.FetchResponse;
import com.kafkalite.server.response.MetadataResponse;
import com.kafkalite.server.response.ProduceResponse;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class ProtocolCodec {

    // ==================== 服务端专用 ====================

    /**
     * 服务端入站解码器：[4字节长度][1字节类型][payload] → Request
     */
    public static class RequestDecoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            // 1. 检查是否够 4 字节（长度字段）
            if (in.readableBytes() < 4) return;

            in.markReaderIndex();
            int length = in.readInt();

            // 2. 检查是否收到完整包
            if (in.readableBytes() < length) {
                in.resetReaderIndex();
                return;
            }

            // 3. 读取类型
            byte typeCode = in.readByte();
            RequestType type = RequestType.fromCode(typeCode);

            // 4. 读取 payload 并构造具体 Request
            ByteBuf payload = in.readSlice(length - 1); // 减去 type 占的1字节
            AbstractRequest request;

            switch (type) {
                case PRODUCE -> request = new ProduceRequest();
                case FETCH -> request = new FetchRequest();
                case METADATA -> request = new MetadataRequest();
                default -> {
                    log.error("Unknown request type: {}", typeCode);
                    return;
                }
            }

            request.decode(payload);
            out.add(request);
        }
    }

    /**
     * 服务端出站编码器：Response → [4字节长度][1字节类型][payload]
     */
    public static class ResponseEncoder extends MessageToByteEncoder<AbstractResponse> {
        @Override
        protected void encode(ChannelHandlerContext ctx, AbstractResponse response, ByteBuf out) {
            // 先写占位长度
            int lengthIdx = out.writerIndex();
            out.writeInt(0);

            // 写入类型字节
            out.writeByte(response.getResponseType().code());

            response.encode(out);

            // 回填长度（总长度 - 4 长度字段本身）
            int totalLen = out.writerIndex() - lengthIdx - 4;
            out.setInt(lengthIdx, totalLen);
        }
    }

    // ==================== 客户端专用====================

    /**
     * 客户端出站编码器：Request → [4字节长度][1字节类型][payload]
     */
    public static class RequestEncoder extends MessageToByteEncoder<AbstractRequest> {
        @Override
        protected void encode(ChannelHandlerContext ctx, AbstractRequest request, ByteBuf out) {
            int lengthIdx = out.writerIndex();
            out.writeInt(0); // 长度占位

            // 1. 写入类型
            out.writeByte(request.getRequestType().code());

            // 2. 写入 payload
            request.encode(out);

            // 3. 回填长度
            int totalLen = out.writerIndex() - lengthIdx - 4;
            out.setInt(lengthIdx, totalLen);
        }
    }

    /**
     * 客户端入站解码器：[4字节长度][1字节类型][payload] → Response
     */
    public static class ResponseDecoder extends ByteToMessageDecoder {
        @Override
        protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
            if (in.readableBytes() < 4) return;

            in.markReaderIndex();
            int length = in.readInt();

            if (in.readableBytes() < length) {
                in.resetReaderIndex();
                return;
            }

            byte typeCode = in.readByte();
            ResponseType type = ResponseType.fromCode(typeCode);
            ByteBuf payload = in.readSlice(length - 1);

            AbstractResponse response;

            switch (type) {
                case PRODUCE -> response = new ProduceResponse();
                case FETCH -> response = new FetchResponse();
                case METADATA -> response = new MetadataResponse();
                default -> {
                    log.error("Unknown request type: {}", typeCode);
                    return;
                }
            }

            response.decode(payload);
            out.add(response);
        }
    }
}
