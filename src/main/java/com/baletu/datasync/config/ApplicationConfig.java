package com.baletu.datasync.config;

import com.alibaba.druid.pool.DruidDataSource;
import com.baletu.datasync.common.DatasourceConfig;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "data.conf")
public class ApplicationConfig {

    private Map<String, DatasourceConfig> srcDataSources;
    private List<CanalAdapter> canalAdapters;

    public void setCanalAdapters(List<CanalAdapter> canalAdapters) {
        this.canalAdapters = canalAdapters;
        /*if (canalAdapters != null) {
            synchronized (DESTINATIONS) {
                DESTINATIONS.clear();
                for (CanalAdapter canalAdapter : canalAdapters) {
                    if (canalAdapter.getInstance() != null) {
                        DESTINATIONS.add(canalAdapter.getInstance());
                    }
                }
            }
        }*/
    }

    public Map<String, DatasourceConfig> getSrcDataSources() {
        return srcDataSources;
    }

    @SuppressWarnings("resource")
    public void setSrcDataSources(Map<String, DatasourceConfig> srcDataSources) {
        this.srcDataSources = srcDataSources;

        if (srcDataSources != null) {
            for (Map.Entry<String, DatasourceConfig> entry : srcDataSources.entrySet()) {
                DatasourceConfig datasourceConfig = entry.getValue();
                // 加载数据源连接池
                DruidDataSource ds = new DruidDataSource();
                ds.setDriverClassName(datasourceConfig.getDriver());
                ds.setUrl(datasourceConfig.getUrl());
                ds.setUsername(datasourceConfig.getUsername());
                ds.setPassword(datasourceConfig.getPassword());
                ds.setInitialSize(1);
                ds.setMinIdle(1);
                ds.setMaxActive(datasourceConfig.getMaxActive());
                ds.setMaxWait(60000);
                ds.setTimeBetweenEvictionRunsMillis(60000);
                ds.setMinEvictableIdleTimeMillis(300000);
                ds.setValidationQuery("select 1");
                try {
                    ds.init();
                } catch (SQLException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
                DatasourceConfig.DATA_SOURCES.put(entry.getKey(), ds);
            }
        }
    }

    public static class CanalAdapter {

        private String      instance; // 实例名

        private List<Group> groups;   // 适配器分组列表

        public String getInstance() {
            return instance;
        }

        public void setInstance(String instance) {
            if (instance != null) {
                this.instance = instance.trim();
            }
        }

        public List<Group> getGroups() {
            return groups;
        }

        public void setGroups(List<Group> groups) {
            this.groups = groups;
        }
    }

    public static class Group {

        // group id
        private String                          groupId          = "default";
        private List<DestDataSourceConfig>        outerAdapters;                           // 适配器列表
        private Map<String, DestDataSourceConfig> outerAdaptersMap = new LinkedHashMap<>();

        public String getGroupId() {
            return groupId;
        }

        public void setGroupId(String groupId) {
            this.groupId = groupId;
        }

        public List<DestDataSourceConfig> getOuterAdapters() {
            return outerAdapters;
        }

        public void setOuterAdapters(List<DestDataSourceConfig> outerAdapters) {
            this.outerAdapters = outerAdapters;
        }

        public Map<String, DestDataSourceConfig> getOuterAdaptersMap() {
            return outerAdaptersMap;
        }

        public void setOuterAdaptersMap(Map<String, DestDataSourceConfig> outerAdaptersMap) {
            this.outerAdaptersMap = outerAdaptersMap;
        }
    }

    public List<CanalAdapter> getCanalAdapters() {
        return canalAdapters;
    }
}
