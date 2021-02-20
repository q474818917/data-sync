package com.baletu.datasync.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.baletu.datasync.common.Result;
import com.baletu.datasync.config.ApplicationConfig;
import com.baletu.datasync.config.DestDataSourceConfig;
import com.baletu.datasync.config.LeafConfig;
import com.baletu.datasync.config.MappingConfig;
import com.baletu.datasync.loader.ConfigLoader;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RdbDataSyncHandle implements DataSyncHandle {

    private static Logger logger              = LoggerFactory.getLogger(RdbDataSyncHandle.class);

    private Map<String, MappingConfig>              rdbMapping          = new ConcurrentHashMap<>();                // 文件名对应配置
    private Map<String, Map<String, MappingConfig>> mappingConfigCache  = new ConcurrentHashMap<>();                // 库名-表名对应配置
    //private Map<String, MirrorDbConfig>             mirrorDbConfigCache = new ConcurrentHashMap<>();                // 镜像库配置

    private DruidDataSource                         dataSource;

    private RdbDataService                          rdbDataService;
    /*private RdbMirrorDbSyncService                  rdbMirrorDbSyncService;

    private RdbConfigMonitor                        rdbConfigMonitor;*/

    private Properties envProperties;

    public Map<String, MappingConfig> getRdbMapping() {
        return rdbMapping;
    }

    public Map<String, Map<String, MappingConfig>> getMappingConfigCache() {
        return mappingConfigCache;
    }

    @Autowired
    private ApplicationConfig applicationConfig;

    @Autowired
    private LeafConfig leafConfig;

    @Override
    public void init(DestDataSourceConfig destDataSourceConfig) {

        Map<String, MappingConfig> rdbMappingTmp = ConfigLoader.load(envProperties);
        // 过滤不匹配的key的配置
        rdbMappingTmp.forEach((key, mappingConfig) -> {
            /*if ((mappingConfig.getOuterAdapterKey() == null && configuration.getKey() == null)
                    || (mappingConfig.getOuterAdapterKey() != null && mappingConfig.getOuterAdapterKey()
                    .equalsIgnoreCase(configuration.getKey()))) {*/
                rdbMapping.put(key, mappingConfig);
            /*}*/
        });

        if (rdbMapping.isEmpty()) {
            //throw new RuntimeException("No rdb adapter found for config key: " + configuration.getKey());
        }

        for (Map.Entry<String, MappingConfig> entry : rdbMapping.entrySet()) {
            String configName = entry.getKey();
            MappingConfig mappingConfig = entry.getValue();
            if (!mappingConfig.getDbMapping().getMirrorDb()) {
                String key;
                if (envProperties != null && !"tcp".equalsIgnoreCase(envProperties.getProperty("canal.conf.mode"))) {
                    key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "-"
                            + StringUtils.trimToEmpty(mappingConfig.getGroupId()) + "_"
                            + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable();
                } else {
                    key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "_"
                            + mappingConfig.getDbMapping().getDatabase() + "-" + mappingConfig.getDbMapping().getTable();
                }
                Map<String, MappingConfig> configMap = mappingConfigCache.computeIfAbsent(key,
                        k1 -> new ConcurrentHashMap<>());
                configMap.put(configName, mappingConfig);
            } /*else {
                // mirrorDB
                String key = StringUtils.trimToEmpty(mappingConfig.getDestination()) + "."
                        + mappingConfig.getDbMapping().getDatabase();
                mirrorDbConfigCache.put(key, MirrorDbConfig.create(configName, mappingConfig));
            }*/
        }

        // 初始化连接池
        Map<String, String> properties = destDataSourceConfig.getProperties();
        dataSource = new DruidDataSource();
        dataSource.setDriverClassName(properties.get("jdbc.driverClassName"));
        dataSource.setUrl(properties.get("jdbc.url"));
        dataSource.setUsername(properties.get("jdbc.username"));
        dataSource.setPassword(properties.get("jdbc.password"));
        dataSource.setInitialSize(1);
        dataSource.setMinIdle(1);
        dataSource.setMaxActive(30);
        dataSource.setMaxWait(60000);
        dataSource.setTimeBetweenEvictionRunsMillis(60000);
        dataSource.setMinEvictableIdleTimeMillis(300000);
        dataSource.setUseUnfairLock(true);
        // List<String> array = new ArrayList<>();
        // array.add("set names utf8mb4;");
        // dataSource.setConnectionInitSqls(array);

        try {
            dataSource.init();
        } catch (SQLException e) {
            logger.error("ERROR ## failed to initial datasource: " + properties.get("jdbc.url"), e);
        }

        String threads = properties.get("threads");
        // String commitSize = properties.get("commitSize");

        /*boolean skipDupException = BooleanUtils.toBoolean(configuration.getProperties()
                .getOrDefault("skipDupException", "true"));
        rdbDataService = new RdbDataService(dataSource,
                threads != null ? Integer.valueOf(threads) : null,
                skipDupException);*/
    }

    @Override
    public Result etl(String task, List<String> params) {
        Result etlResult = new Result();
        MappingConfig mappingConfig = rdbMapping.get(task);
        RdbDataService rdbDataService = new RdbDataService(dataSource, mappingConfig, leafConfig);
        if (mappingConfig != null) {
            return rdbDataService.importData(params);
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSucc = true;
            for (MappingConfig configTmp : rdbMapping.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    Result etlRes = rdbDataService.importData(params);
                    if (!etlRes.getSucceeded()) {
                        resSucc = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSucc);
                if (resSucc) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }
}
