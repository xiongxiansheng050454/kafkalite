package com.kafkalite.client;

import com.kafkalite.model.Message;
import com.kafkalite.server.request.FetchRequest;
import com.kafkalite.server.response.FetchResponse;
import lombok.SneakyThrows;

import java.util.List;

public class SimpleConsumer {
    private final NetworkClient client;

    public SimpleConsumer(String host, int port) throws InterruptedException {
        this.client = new NetworkClient(host, port);
        this.client.connect();
    }

    @SneakyThrows
    public List<Message> poll(String topic, int partition, long offset, int maxBytes) {
        FetchRequest req = new FetchRequest();
        req.setTopic(topic);
        req.setPartition(partition);
        req.setOffset(offset);
        req.setMaxBytes(maxBytes);

        FetchResponse resp = (FetchResponse) client.send(req).get();
        if (resp.getErrorCode() != 0) {
            throw new RuntimeException("Fetch failed: " + resp.getErrorCode());
        }
        return resp.getMessageSet().getMessages();
    }

    public void close() { client.close(); }
}