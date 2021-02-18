package com.baletu.datasync.client.process;

import com.alibaba.otter.canal.protocol.FlatMessage;

public interface MessageHandle {

    void execute(FlatMessage message);
}
