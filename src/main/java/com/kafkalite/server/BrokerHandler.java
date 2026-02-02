package com.kafkalite.server;

import com.kafkalite.model.Partition;
import com.kafkalite.model.Topic;
import com.kafkalite.server.request.AbstractRequest;
import com.kafkalite.server.request.FetchRequest;
import com.kafkalite.server.request.MetadataRequest;
import com.kafkalite.server.request.ProduceRequest;
import com.kafkalite.server.response.AbstractResponse;
import com.kafkalite.server.response.FetchResponse;
import com.kafkalite.server.response.MetadataResponse;
import com.kafkalite.server.response.ProduceResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class BrokerHandler extends SimpleChannelInboundHandler<AbstractRequest> {
    // 静态共享，所有连接共享同一个 Topic 管理器
    private static final Map<String, Topic> topics = new ConcurrentHashMap<>();

    private static final int DEFAULT_PARTITIONS = 1;// 默认单分区

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, AbstractRequest request) {
        log.info("Received request: {}", request.getClass().getSimpleName());

        AbstractResponse response;
        try {
            if (request instanceof ProduceRequest) {
                response = handleProduce((ProduceRequest) request);
            } else if (request instanceof FetchRequest) {
                response = handleFetch((FetchRequest) request);
            } else if (request instanceof MetadataRequest) {
                response = handleMetadata((MetadataRequest) request);
            } else {
                response = new ProduceResponse();
                response.setErrorCode(AbstractResponse.UNKNOWN_TOPIC);
            }
        } catch (Exception e) {
            log.error("Error handling request", e);
            response = new ProduceResponse();
            response.setErrorCode((short) 99); // 内部错误
        }

        ctx.writeAndFlush(response);
    }

    private ProduceResponse handleProduce(ProduceRequest req) {
        ProduceResponse resp = new ProduceResponse();

        Topic topic = topics.computeIfAbsent(req.getTopic(),
                name -> new Topic(name, DEFAULT_PARTITIONS));

        Partition partition = topic.getPartition(req.getPartition());
        if (partition == null) {
            log.warn("Partition {} not found in topic {}", req.getPartition(), req.getTopic());
            resp.setErrorCode(AbstractResponse.UNKNOWN_TOPIC);
            return resp;
        }

        // 复用 Day1 的写入逻辑
        partition.append(req.getMessageSet());
        resp.setOffset(partition.logEndOffset());

        log.info("Produced {} messages to {}-{}, offset: {}",
                req.getMessageSet().size(), req.getTopic(), req.getPartition(), resp.getOffset());
        return resp;
    }

    private FetchResponse handleFetch(FetchRequest req) {
        FetchResponse resp = new FetchResponse();

        Topic topic = topics.get(req.getTopic());
        if (topic == null) {
            resp.setErrorCode(AbstractResponse.UNKNOWN_TOPIC);
            return resp;
        }

        Partition partition = topic.getPartition(req.getPartition());
        if (partition == null) {
            resp.setErrorCode(AbstractResponse.UNKNOWN_TOPIC);
            return resp;
        }

        // 复用 Day1 的读取逻辑
        var messages = partition.fetch(req.getOffset(), req.getMaxBytes());
        resp.setMessages(messages);
        resp.setHighWatermark(partition.logEndOffset());

        log.info("Fetched {} messages from {}-{} at offset {}",
                messages.size(), req.getTopic(), req.getPartition(), req.getOffset());
        return resp;
    }

    private MetadataResponse handleMetadata(MetadataRequest req) {
        MetadataResponse resp = new MetadataResponse();

        if (req.getTopic() == null || req.getTopic().isEmpty()) {
            // 返回所有 topic
            topics.forEach((name, topic) ->
                    resp.getTopicMetadata().put(name, topic.getNumPartitions()));
        } else {
            Topic topic = topics.get(req.getTopic());
            if (topic != null) {
                resp.getTopicMetadata().put(topic.getName(), topic.getNumPartitions());
            }
        }
        return resp;
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Broker error", cause);
        ctx.close();
    }
}
