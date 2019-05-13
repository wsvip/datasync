package com.ws.datasync.common.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ConfigUtil {

    protected final static Logger logger = LoggerFactory.getLogger(ConfigUtil.class);

    private String configPath = "/syncinf.properties";
    private Properties props;

    @PostConstruct
    public void init() {
        loadConfig();
    }

    public String getProperty(String key) {
        return props.getProperty(key);
    }

    public Set<Object> keys() {
        return props.keySet();
    }

    private void loadConfig() {

        InputStream is = null;
        try {
            is = ConfigUtil.class.getResourceAsStream(configPath);
            Properties configProps = new Properties();
            configProps.load(is);
            props = configProps;
        } catch (Exception e) {
            logger.error("Failed to load " + configPath, e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                }
            }
        }
    }

    public void setProperty(String key, String value) {
        props.setProperty(key, value);
    }

    /**
     * 保存到文件(同步操作)
     */
    public synchronized void save() {

        FileWriter fw = null;
        try {
            File config = new File(ConfigUtil.class.getResource(configPath).toURI());
            fw = new FileWriter(config);
            props.store(fw, "");
        } catch (Exception e) {
            logger.error("Failed to save " + configPath, e);
        } finally {
            try {
                if (fw != null) {
                    fw.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
