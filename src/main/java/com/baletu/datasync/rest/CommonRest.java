package com.baletu.datasync.rest;

import com.baletu.datasync.client.DataRocketMQClient;
import com.baletu.datasync.common.EtlLock;
import com.baletu.datasync.common.Result;
import com.baletu.datasync.config.ApplicationConfig;
import com.baletu.datasync.service.DataSyncHandle;
import com.baletu.datasync.service.RdbDataSyncHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;


@RestController
public class CommonRest {

    private static Logger                 logger           = LoggerFactory.getLogger(CommonRest.class);

    private static final String           ETL_LOCK_ZK_NODE = "/sync-etl/";

    @Resource
    private EtlLock etlLock;

    @Autowired
    private DataSyncHandle dataSyncHandle;

    /**
     * ETL curl http://127.0.0.1:8081/oracle1/mytest_user.yml -X POST
     *
     * @param task 任务名对应配置文件名 mytest_user.yml
     * @param params etl where条件参数, 为空全部导入
     */
    @PostMapping("/etl/{type}/{key}/{task}")
    public Result sync(@PathVariable String task,
                       @RequestParam(name = "params", required = false) String params) {
        /*String lockKey = destination == null ? task : destination;
        boolean locked = etlLock.tryLock(ETL_LOCK_ZK_NODE + type + "-" + lockKey);
        if (!locked) {
            EtlResult result = new EtlResult();
            result.setSucceeded(false);
            result.setErrorMessage(task + " 有其他进程正在导入中, 请稍后再试");
            return result;
        }*/
        try {
            try {
                List<String> paramArray = null;
                if (params != null) {
                    paramArray = Arrays.asList(params.trim().split(";"));
                }
                return dataSyncHandle.etl(task, paramArray);
            } finally {

            }
        } finally {
            //etlLock.unlock(ETL_LOCK_ZK_NODE + type + "-" + lockKey);
        }
    }

    /**
     * 启动Binlog MQ 监听
     * @return
     */
    @GetMapping("/binlog/start")
    public Result syncSwitch() {
        Result result = new Result();
        try {
            DataRocketMQClient.lock.lock();
            DataRocketMQClient.condition.signalAll();
            result.setSucceeded(true);
            result.setResultMessage("操作成功");
        }catch(Exception e) {
            logger.error(e.getMessage(), e);
            result.setSucceeded(false);
            result.setResultMessage("操作失败");
        }finally {
            DataRocketMQClient.lock.unlock();
        }

        return result;

    }
}
