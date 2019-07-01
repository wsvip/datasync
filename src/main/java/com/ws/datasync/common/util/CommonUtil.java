package com.ws.datasync.common.util;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

/**
 * @Author:  WS-
 * @return:
 * @Date:    2019/6/10  10:19
 * @Description:
 */
public class CommonUtil {

    public static List<Map<String, Object>> getFildCountList(String tableName, String primaryDBType, String secondaryDBType, JdbcTemplate jdbcTemplate){
        //查询表字段数量
        String fildCountSql = null;
        if ("oracle".equalsIgnoreCase(secondaryDBType)) {
            fildCountSql = "SELECT A.COLUMN_NAME FROM USER_TAB_COLUMNS A WHERE TABLE_NAME ='" + tableName.toUpperCase() + "'";
        } else if ("mysql".equalsIgnoreCase(secondaryDBType)) {
            fildCountSql = "select COLUMN_NAME from information_schema.columns where table_name ='" + tableName + "'";
        } else {

        }
        List<Map<String, Object>> fildCountList = jdbcTemplate.queryForList(fildCountSql);
        return fildCountList;
    }
}
