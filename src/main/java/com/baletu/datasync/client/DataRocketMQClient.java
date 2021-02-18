package com.baletu.datasync.client;

import com.alibaba.otter.canal.client.rocketmq.RocketMQCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.alibaba.otter.canal.protocol.Message;
import com.baletu.datasync.client.process.DataMessageOperation;
import com.baletu.datasync.client.process.MessageHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import java.util.List;
import java.util.concurrent.TimeUnit;


public class DataRocketMQClient {

    protected final static Logger logger  = LoggerFactory.getLogger(DataRocketMQClient.class);

    private RocketMQCanalConnector          connector;

    public static volatile boolean         running = false;

    private Thread thread  = null;

    private Thread.UncaughtExceptionHandler handler = (t, e) -> logger.error("parse events has an error", e);

    private MessageHandle messageHandle;

    public DataRocketMQClient(String nameServers, String topic, String groupId) {
        connector = new RocketMQCanalConnector(nameServers, topic, groupId, 500, true);
    }

    public DataRocketMQClient(String nameServers, String topic, String groupId, boolean enableMessageTrace,
                              String accessKey, String secretKey, String accessChannel, String namespace) {
        connector = new RocketMQCanalConnector(nameServers, topic, groupId, accessKey,
            secretKey, -1, true, enableMessageTrace,
            null, accessChannel, namespace);
    }

    public void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);
        thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    public void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    private void process() {
        while (!running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
        }

        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    List<FlatMessage> messages = connector.getFlatList(100L, TimeUnit.MILLISECONDS); // 获取message
                    for (FlatMessage message : messages) {
                        long batchId = message.getId();
                        if (batchId == -1 || message.getData() == null) {
                            // try {
                            // Thread.sleep(1000);
                            // } catch (InterruptedException e) {
                            // }
                        } else {
                            switch(message.getType()) {
                                case "UPDATE":
                                    this.changeHandleType(DataMessageOperation.HandleType.UPDATE);
                                    break;
                                case "INSERT":
                                    this.changeHandleType(DataMessageOperation.HandleType.INSERT);
                                    break;
                                case "DELETE":
                                    this.changeHandleType(DataMessageOperation.HandleType.DELETE);
                                    break;
                            }
                            this.messageHandle.execute(message);
                        }
                    }

                    connector.ack(); // 提交确认
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        connector.unsubscribe();
        // connector.stopRunning();
    }

    private void changeHandleType(MessageHandle messageHandle) {
        this.messageHandle = messageHandle;
    }

}
