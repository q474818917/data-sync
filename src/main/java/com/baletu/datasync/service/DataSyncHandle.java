package com.baletu.datasync.service;

import com.baletu.datasync.common.Result;
import com.baletu.datasync.config.ApplicationConfig;
import com.baletu.datasync.config.DestDataSourceConfig;

import java.util.List;

public interface DataSyncHandle {

    void init(DestDataSourceConfig destDataSourceConfig);

    default Result etl(String task, List<String> params) {
        throw new UnsupportedOperationException("unsupported operation");
    }
}
