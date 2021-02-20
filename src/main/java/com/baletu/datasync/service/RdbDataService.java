package com.baletu.datasync.service;

import com.baletu.datasync.common.Result;
import com.baletu.datasync.config.ApplicationConfig;
import com.baletu.datasync.config.LeafConfig;
import com.baletu.datasync.config.MappingConfig;
import com.baletu.datasync.support.SyncUtil;
import com.baletu.datasync.support.Util;
import com.google.common.collect.Lists;
import com.sankuai.inf.leaf.service.SegmentService;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class RdbDataService extends AbstractDataService {

    private static final Logger logger  = LoggerFactory.getLogger(RdbDataService.class);

    private DataSource    targetDS;
    private MappingConfig config;

    public RdbDataService(DataSource targetDS, MappingConfig mappingConfig, LeafConfig leafConfig){
        super(mappingConfig, leafConfig);
        this.targetDS = targetDS;
        this.config = mappingConfig;
    }

    /**
     * 导入数据
     */
    public Result importData(List<String> params) {
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        //String sql = "SELECT * FROM " + dbMapping.getDatabase() + "." + dbMapping.getTable();
        String sql = dbMapping.getSql();
        return importData(sql, params);
    }

    /**
     * 执行导入
     */
    protected boolean executeSqlImport(DataSource srcDS, String sql, List<Object> values, MappingConfig.DbMapping dbMapping, AtomicLong impCount, List<String> errMsg) {
        try {
            //MappingConfig.DbMapping dbMapping = (MappingConfig.DbMapping) mapping;
            Map<String, String> columnsMap = new LinkedHashMap<>();
            Map<String, Integer> columnType = new LinkedHashMap<>();

            //执行目标表需要导入的字段
            Util.sqlRS(targetDS, "SELECT * FROM " + SyncUtil.getDbTableName(dbMapping) + " LIMIT 1 ", rs -> {
                try {

                    ResultSetMetaData rsd = rs.getMetaData();
                    int columnCount = rsd.getColumnCount();
                    List<String> columns = new ArrayList<>();
                    for (int i = 1; i <= columnCount; i++) {
                        columnType.put(rsd.getColumnName(i).toLowerCase(), rsd.getColumnType(i));
                        columns.add(rsd.getColumnName(i));
                    }

                    columnsMap.putAll(SyncUtil.getColumnsMap(dbMapping, columns));
                    return true;
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    return false;
                }
            });

            //执行原表数据，根据上一步的字段进行拼接SQL
            Util.sqlRS(srcDS, sql, values, rs -> {
                List<String> keyColumnList = Lists.newArrayList();
                if(StringUtils.isNotEmpty(dbMapping.getTargetIdKey())) {
                    keyColumnList = Arrays.asList(dbMapping.getTargetIdKey().split(","));
                }
                int idx = 1;

                try {
                    boolean completed = false;

                    StringBuilder insertSql = new StringBuilder();
                    insertSql.append("INSERT INTO ").append(SyncUtil.getDbTableName(dbMapping)).append(" (");
                    columnsMap
                            .forEach((targetColumnName, srcColumnName) -> insertSql.append(targetColumnName).append(","));

                    int len = insertSql.length();
                    insertSql.delete(len - 1, len).append(") VALUES (");
                    int mapLen = columnsMap.size();
                    for (int i = 0; i < mapLen; i++) {
                        insertSql.append("?,");
                    }
                    len = insertSql.length();
                    insertSql.delete(len - 1, len).append(")");
                    try (Connection connTarget = targetDS.getConnection();
                         PreparedStatement pstmt = connTarget.prepareStatement(insertSql.toString())) {
                        connTarget.setAutoCommit(false);

                        while (rs.next()) {
                            completed = false;

                            Map<String, Object> sourceValueMap = new LinkedHashMap<>();
                            Map<String, Object> targetValueMap = new LinkedHashMap<>();

                            pstmt.clearParameters();

                            // 删除数据
                            Map<String, Object> pkVal = new LinkedHashMap<>();
                            StringBuilder deleteSql = new StringBuilder(
                                    "DELETE FROM " + SyncUtil.getDbTableName(dbMapping) + " WHERE ");
                            appendCondition(dbMapping, deleteSql, pkVal, rs);
                            try (PreparedStatement pstmt2 = connTarget.prepareStatement(deleteSql.toString())) {
                                int k = 1;
                                for (Object val : pkVal.values()) {
                                    pstmt2.setObject(k++, val);
                                }
                                pstmt2.execute();
                            }

                            int i = 1;
                            for (Map.Entry<String, String> entry : columnsMap.entrySet()) {
                                String targetClolumnName = entry.getKey();
                                String srcColumnName = entry.getValue();
                                if (srcColumnName == null) {
                                    srcColumnName = targetClolumnName;
                                }

                                Integer type = columnType.get(targetClolumnName.toLowerCase());

                                Object value = null;

                                if(keyColumnList.contains(targetClolumnName.toLowerCase())) {
                                    com.sankuai.inf.leaf.common.Result result = segmentService.getIdGen().get("leaf-segment-test");
                                    Object sourceValue = rs.getObject(srcColumnName);
                                    value = result.getId();
                                    sourceValueMap.put(srcColumnName, sourceValue);
                                    targetValueMap.put(targetClolumnName, value);
                                }else {
                                    value = rs.getObject(srcColumnName);
                                    sourceValueMap.put(srcColumnName, value);
                                    targetValueMap.put(targetClolumnName, value);
                                }

                                if (value != null) {
                                    SyncUtil.setPStmt(type, pstmt, value, i);
                                } else {
                                    pstmt.setNull(i, type);
                                }

                                i++;
                            }
                            handleMappingTable(connTarget, sourceValueMap, targetValueMap);
                            pstmt.execute();
                            if (logger.isTraceEnabled()) {
                                logger.trace("Insert into target table, sql: {}", insertSql);
                            }

                            if (idx % dbMapping.getCommitBatch() == 0) {
                                connTarget.commit();
                                completed = true;
                            }
                            idx++;
                            impCount.incrementAndGet();
                            if (logger.isDebugEnabled()) {
                                logger.debug("successful import count:" + impCount.get());
                            }
                        }
                        if (!completed) {
                            connTarget.commit();
                        }
                    }

                } catch (Exception e) {
                    logger.error(dbMapping.getTable() + " etl failed! ==>" + e.getMessage(), e);
                    errMsg.add(dbMapping.getTable() + " etl failed! ==>" + e.getMessage());
                }
                return idx;
            });
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * 拼接目标表主键where条件
     */
    private static void appendCondition(MappingConfig.DbMapping dbMapping, StringBuilder sql, Map<String, Object> values,
                                        ResultSet rs) throws SQLException {
        // 拼接主键
        for (Map.Entry<String, String> entry : dbMapping.getTargetPk().entrySet()) {
            String targetColumnName = entry.getKey();
            String srcColumnName = entry.getValue();
            if (srcColumnName == null) {
                srcColumnName = targetColumnName;
            }
            sql.append(targetColumnName).append("=? AND ");
            values.put(targetColumnName, rs.getObject(srcColumnName));
        }
        int len = sql.length();
        sql.delete(len - 4, len);
    }

    private void handleMappingTable(Connection connTarget,
            Map<String, Object> sourceValueMap, Map<String, Object> targetValueMap) {
        MappingConfig.UseMapping useMapping = this.config.getUseMapping();
        Map<String, String> columnsMap = new LinkedHashMap<>();
        try {
            columnsMap = useMapping.getColumns();
            StringBuilder insertSql = new StringBuilder();
            if(useMapping != null) {

                insertSql.append("INSERT INTO ").append(useMapping.getTable()).append(" (");
                columnsMap
                        .forEach((targetColumnName, srcColumnName) -> insertSql.append(targetColumnName).append(","));

                int len = insertSql.length();
                insertSql.delete(len - 1, len).append(") VALUES (");
                int mapLen = columnsMap.size();
                for (int i = 0; i < mapLen; i++) {
                    insertSql.append("?,");
                }
                len = insertSql.length();
                insertSql.delete(len - 1, len).append(")");
            }

            try (PreparedStatement pstmt = connTarget.prepareStatement(insertSql.toString())) {
                connTarget.setAutoCommit(false);

                int i = 1;

                for(Map.Entry<String, Object> entry : sourceValueMap.entrySet()) {
                    String sourceClolumnName = entry.getKey();
                    Object sourceColumnValue = entry.getValue();
                    if(columnsMap.values().contains(sourceClolumnName)) {
                        SyncUtil.setPStmt(Types.BIGINT, pstmt, sourceColumnValue, i);
                        i++;
                    }
                }
                for (Map.Entry<String, Object> entry : targetValueMap.entrySet()) {
                    String targetClolumnName = entry.getKey();
                    Object targetColumnValue = entry.getValue();
                    if(columnsMap.values().contains(targetClolumnName)) {
                        SyncUtil.setPStmt(Types.BIGINT, pstmt, targetColumnValue, i);
                        i++;
                    }
                }
                if(columnsMap.keySet().contains("source")) {
                    SyncUtil.setPStmt(Types.BIGINT, pstmt, columnsMap.get("source"), i);
                }

                pstmt.execute();
                if (logger.isTraceEnabled()) {
                    logger.trace("Insert into target table, sql: {}", insertSql);
                }

                /*if (idx % dbMapping.getCommitBatch() == 0) {
                    connTarget.commit();
                    completed = true;
                }
                idx++;
                impCount.incrementAndGet();
                if (logger.isDebugEnabled()) {
                    logger.debug("successful import count:" + impCount.get());
                }*/
            }
            /*if (!completed) {
                connTarget.commit();
            }*/

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }
}
