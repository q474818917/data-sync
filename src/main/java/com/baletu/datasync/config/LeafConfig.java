package com.baletu.datasync.config;

import com.sankuai.inf.leaf.common.PropertyFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "leaf",ignoreUnknownFields = true)
public class LeafConfig {
    private String name;
    private Segment segment;
    public static class Segment{
        private boolean enable=false;
        private String url;
        private String username;
        private String password;

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public boolean isEnable() {
            return enable;
        }
        public void setEnable(boolean enable) {
            this.enable = enable;
        }

        @Override
        public String toString() {
            return "Segment{" +
                    "enable=" + enable +
                    '}';
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        PropertyFactory.setLeafName(name);
        this.name = name;
    }

    public Segment getSegment() {
        return segment;
    }

    public void setSegment(Segment segment) {
        this.segment = segment;
    }

    @Override
    public String toString() {
        return "LeafSpringBootProperties{" +
                "name='" + name + '\'' +
                ", segment=" + segment +
                '}';
    }
}
