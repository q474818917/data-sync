package com.baletu.datasync.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.baletu.datasync.common.DatasourceConfig;
import com.baletu.datasync.common.Result;
import com.baletu.datasync.config.ApplicationConfig;
import com.baletu.datasync.config.MappingConfig;
import com.baletu.datasync.support.Util;
import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public abstract class AbstractDataService {

    protected Logger logger       = LoggerFactory.getLogger(this.getClass());

    private String        type;
    private MappingConfig mapping;
    private final long    CNT_PER_TASK = 10000L;

    public AbstractDataService(MappingConfig mapping){
        this.mapping = mapping;
    }

    protected Result importData(String sql, List<String> params) {
        Result etlResult = new Result();
        AtomicLong impCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        if (mapping == null) {
            logger.warn("{} mapping config is null, etl go end ", type);
            etlResult.setErrorMessage(type + "mapping config is null, etl go end ");
            return etlResult;
        }

        long start = System.currentTimeMillis();
        try {
            DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(mapping.getDataSourceKey());
            List<Object> values = new ArrayList<>();
            // 拼接条件
            /*if (config.getMapping().getEtlCondition() != null && params != null) {
                String etlCondition = config.getMapping().getEtlCondition();
                for (String param : params) {
                    etlCondition = etlCondition.replace("{}", "?");
                    values.add(param);
                }

                sql += " " + etlCondition;
            }*/

            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", sql);
            }

            // 获取总数
            String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
            long cnt = (Long) Util.sqlRS(dataSource, countSql, values, rs -> {
                Long count = null;
                try {
                    if (rs.next()) {
                        count = ((Number) rs.getObject(1)).longValue();
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                return count == null ? 0L : count;
            });

            // 当大于1万条记录时开启多线程
            if (cnt >= 10000) {
                int threadCount = Runtime.getRuntime().availableProcessors();

                long offset;
                long size = CNT_PER_TASK;
                long workerCnt = cnt / size + (cnt % size == 0 ? 0 : 1);

                if (logger.isDebugEnabled()) {
                    logger.debug("workerCnt {} for cnt {} threadCount {}", workerCnt, cnt, threadCount);
                }

                ExecutorService executor = Util.newFixedThreadPool(threadCount, 5000L);
                List<Future<Boolean>> futures = new ArrayList<>();
                for (long i = 0; i < workerCnt; i++) {
                    offset = size * i;
                    String sqlFinal = sql + " LIMIT " + offset + "," + size;
                    Future<Boolean> future = executor.submit(() -> executeSqlImport(dataSource,
                            sqlFinal,
                            values,
                            mapping.getDbMapping(),
                            impCount,
                            errMsg));
                    futures.add(future);
                }

                for (Future<Boolean> future : futures) {
                    future.get();
                }
                executor.shutdown();
            } else {
                executeSqlImport(dataSource, sql, values, mapping.getDbMapping(), impCount, errMsg);
            }

            logger.info("数据全量导入完成, 一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
            etlResult.setResultMessage("导入" + type + " 数据：" + impCount.get() + " 条");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add(type + " 数据导入异常 =>" + e.getMessage());
        }
        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
    }

    protected abstract boolean executeSqlImport(DataSource ds, String sql, List<Object> values,
                                                MappingConfig.DbMapping mapping, AtomicLong impCount,
                                                List<String> errMsg);
}