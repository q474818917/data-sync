package com.baletu.datasync.service;

import com.baletu.datasync.config.ApplicationConfig;
import com.baletu.datasync.config.DestDataSourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

@Component
public class DataSyncService {

    private static final Logger logger  = LoggerFactory.getLogger(DataSyncService.class);

    @Resource
    private ApplicationConfig applicationConfig;

    private volatile boolean    running = false;

    @Autowired
    private DataSyncHandle dataSyncHandle;

    @PostConstruct
    public synchronized void init() {
        if (running) {
            return;
        }
        try {
            logger.info("## syncSwitch refreshed.");
            logger.info("## start the canal client adapters.");

            for(ApplicationConfig.CanalAdapter canalAdapter : applicationConfig.getCanalAdapters()) {
                for(ApplicationConfig.Group group : canalAdapter.getGroups()) {
                    for(DestDataSourceConfig destDataSourceConfig : group.getOuterAdapters()) {
                        dataSyncHandle.init(destDataSourceConfig);
                    }
                }
            }
            running = true;
            logger.info("## the canal client adapters are running now ......");
        } catch (Exception e) {
            logger.error("## something goes wrong when starting up the canal client adapters:", e);
        }
    }
}
