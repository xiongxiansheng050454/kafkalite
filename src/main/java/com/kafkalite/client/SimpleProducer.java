package com.kafkalite.client;

import com.kafkalite.model.Message;
import com.kafkalite.model.MessageSet;
import com.kafkalite.server.request.ProduceRequest;
import com.kafkalite.server.response.ProduceResponse;
import lombok.SneakyThrows;

public class SimpleProducer {
    private final NetworkClient client;

    public SimpleProducer(String host, int port) throws InterruptedException {
        this.client = new NetworkClient(host, port);
        this.client.connect();
    }

    @SneakyThrows
    public long send(String topic, int partition, String key, String value) {
        Message msg = new Message(key.getBytes(), value.getBytes());
        MessageSet set = new MessageSet();
        set.append(msg);

        ProduceRequest req = new ProduceRequest();
        req.setTopic(topic);
        req.setPartition(partition);
        req.setMessageSet(set);

        ProduceResponse resp = (ProduceResponse) client.send(req).get();
        if (resp.getErrorCode() != 0) {
            throw new RuntimeException("Produce failed with error: " + resp.getErrorCode());
        }
        return resp.getOffset();
    }

    public void close() { client.close(); }
}