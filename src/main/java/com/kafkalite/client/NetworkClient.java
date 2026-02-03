package com.kafkalite.client;

import com.kafkalite.server.protocol.ProtocolCodec;
import com.kafkalite.server.request.AbstractRequest;
import com.kafkalite.server.response.AbstractResponse;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class NetworkClient {
    private final String host;
    private final int port;
    private Channel channel;
    private final EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
    private final ResponseHandler responseHandler = new ResponseHandler();
    private final AtomicLong correlationIdGenerator = new AtomicLong(0); // ID生成器

    public NetworkClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void connect() throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new ProtocolCodec.RequestEncoder());
                        p.addLast(new ProtocolCodec.ResponseDecoder());
                        p.addLast(responseHandler);
                    }
                });

        channel = bootstrap.connect(host, port).sync().channel();
        log.info("Connected to broker at {}:{}", host, port);
    }

    public CompletableFuture<AbstractResponse> send(AbstractRequest request) {
        // 分配唯一ID
        long correlationId = correlationIdGenerator.getAndIncrement();
        request.setCorrelationId(correlationId);

        CompletableFuture<AbstractResponse> future = new CompletableFuture<>();
        responseHandler.registerPending(correlationId, future);

        channel.writeAndFlush(request).addListener((ChannelFutureListener) f -> {
            if (!f.isSuccess()) {
                responseHandler.removePending(correlationId); // 发送失败时清理
                future.completeExceptionally(f.cause());
            }
        });

        return future.orTimeout(5, TimeUnit.SECONDS);
    }

    public void close() {
        if (channel != null) {
            channel.close();
        }
        group.shutdownGracefully();
    }

    @Slf4j
    static class ResponseHandler extends SimpleChannelInboundHandler<AbstractResponse> {
        // 使用 correlationId 映射
        private final Map<Long, CompletableFuture<AbstractResponse>> pending = new ConcurrentHashMap<>();

        public void registerPending(long correlationId, CompletableFuture<AbstractResponse> future) {
            pending.put(correlationId, future);
        }

        public void removePending(long correlationId) {
            pending.remove(correlationId);
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, AbstractResponse response) {
            long correlationId = response.getCorrelationId();
            CompletableFuture<AbstractResponse> future = pending.remove(correlationId);

            if (future != null) {
                future.complete(response);
            } else {
                log.warn("Received response for unknown correlationId: {}", correlationId);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("Network error", cause);
            ctx.close();
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.warn("Channel inactive, failing all pending requests");
            pending.forEach((id, future) -> {
                if (!future.isDone()) {
                    future.completeExceptionally(new RuntimeException("Connection closed"));
                }
            });
            pending.clear();
        }
    }
}