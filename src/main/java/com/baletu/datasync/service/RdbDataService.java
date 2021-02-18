package com.baletu.datasync.service;

import com.baletu.datasync.common.Result;
import com.baletu.datasync.config.ApplicationConfig;
import com.baletu.datasync.config.MappingConfig;
import com.baletu.datasync.support.SyncUtil;
import com.baletu.datasync.support.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RdbDataService extends AbstractDataService {

    private static final Logger logger  = LoggerFactory.getLogger(RdbDataService.class);

    private DataSource    targetDS;
    private MappingConfig config;

    public RdbDataService(DataSource targetDS, MappingConfig config){
        super(config);
        this.targetDS = targetDS;
        this.config = config;
    }

    /**
     * 导入数据
     */
    public Result importData(List<String> params) {
        MappingConfig.DbMapping dbMapping = config.getDbMapping();
        String sql = "SELECT * FROM " + dbMapping.getDatabase() + "." + dbMapping.getTable();
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

                                Object value = rs.getObject(srcColumnName);
                                if (value != null) {
                                    SyncUtil.setPStmt(type, pstmt, value, i);
                                } else {
                                    pstmt.setNull(i, type);
                                }

                                i++;
                            }

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


}
