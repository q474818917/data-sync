package com.baletu.datasync.service;

import com.baletu.datasync.common.Result;
import com.baletu.datasync.config.DestDataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LoggerHandle implements DataSyncHandle {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Override
    public void init(DestDataSourceConfig destDataSourceConfig) {

    }

    @Override
    public Result etl(String task, List<String> params) {
        return null;
    }
}
