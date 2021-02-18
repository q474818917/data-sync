package com.baletu.datasync.loader;

import com.baletu.datasync.config.MappingConfig;
import com.baletu.datasync.config.YmlConfigBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;


public class ConfigLoader {

    private static Logger logger = LoggerFactory.getLogger(ConfigLoader.class);

    /**
     * 加载RDB表映射配置
     *
     * @return 配置名/配置文件名--对象
     */
    public static Map<String, MappingConfig> load(Properties envProperties) {
        logger.info("## Start loading rdb mapping config ... ");

        Map<String, MappingConfig> result = new LinkedHashMap<>();
        //加载rdb目录下的yml配置
        Map<String, String> configContentMap = MappingConfigsLoader.loadConfigs("rdb");
        configContentMap.forEach((fileName, content) -> {
            //MappingConfig对应rdb下yml解析后的属性
            MappingConfig config = YmlConfigBinder
                .bindYmlToObj(null, content, MappingConfig.class, null, envProperties);
            if (config == null) {
                return;
            }
            try {
                config.validate();
            } catch (Exception e) {
                throw new RuntimeException("ERROR Config: " + fileName + " " + e.getMessage(), e);
            }
            result.put(fileName, config);
        });

        logger.info("## Rdb mapping config loaded");
        return result;
    }
}
