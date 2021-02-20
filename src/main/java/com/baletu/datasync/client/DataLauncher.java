package com.baletu.datasync.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class DataLauncher {
    protected final static Logger logger  = LoggerFactory.getLogger(DataLauncher.class);

    @PostConstruct
    public void init() {
        try {
            final DataRocketMQClient rocketMQClient = new DataRocketMQClient("localhost:9876",
                    "example",
                    "group",
                    false,
                    "",
                    "",
                    "local",
                    "");
            //logger.info("## Start the rocketmq consumer: {}-{}", topic, groupId);
            rocketMQClient.start();
            logger.info("## The canal rocketmq consumer is running now ......");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    logger.info("## Stop the rocketmq consumer");
                    rocketMQClient.stop();
                } catch (Throwable e) {
                    logger.warn("## Something goes wrong when stopping rocketmq consumer:", e);
                } finally {
                    logger.info("## Rocketmq consumer is down.");
                }
            }));
            /*while (DataRocketMQClient.running)
                ;*/
        } catch (Throwable e) {
            logger.error("## Something going wrong when starting up the rocketmq consumer:", e);
            System.exit(0);
        }
    }
}
