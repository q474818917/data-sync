package com.baletu.datasync.rest;

import com.baletu.datasync.common.EtlLock;
import com.baletu.datasync.common.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;


@RestController
public class CommonRest {

    private static Logger                 logger           = LoggerFactory.getLogger(CommonRest.class);

    private static final String           ETL_LOCK_ZK_NODE = "/sync-etl/";

    @Resource
    private EtlLock etlLock;



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
                //return adapter.sync(task, paramArray);
                return null;
            } finally {

            }
        } finally {
            //etlLock.unlock(ETL_LOCK_ZK_NODE + type + "-" + lockKey);
        }
    }
}
