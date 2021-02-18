package com.baletu.datasync.client.process;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.baletu.datasync.client.DataRocketMQClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataMessageOperation {
    protected final static Logger logger  = LoggerFactory.getLogger(DataMessageOperation.class);

    public enum HandleType implements MessageHandle {
        INSERT((FlatMessage message) -> logger.info("insert message is {}", JSONObject.toJSONString(message))),
        UPDATE((FlatMessage message) -> logger.info("update message is {}", JSONObject.toJSONString(message))),
        DELETE((FlatMessage message) -> logger.info("delete message is {}", JSONObject.toJSONString(message)));

        private final MessageHandle messageHandle;

        HandleType(MessageHandle messageHandle) {
            this.messageHandle = messageHandle;
        }

        @Override
        public void execute(FlatMessage message) {
            messageHandle.execute(message);
        }
    }
}
