package com.ws.datasync.common.util;

import com.ws.datasync.bean.Column;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Mysql2OracleUtil {
    /**
     * 获取mysql表字段名称、字段类型、字段长度、字段实际长度、小数位、是否为空、注释、主键
     * @Author:  WS-
     * @return:
     * @Date:    2019/5/13  20:37
     * @Description:
     */
    public static ArrayList<Column> getMysqlColumn(String tableName, JdbcTemplate jdbcTemplate) {
        ArrayList<Column> columnList = new ArrayList<>();
        Column column = null;
        String sql = "select COLUMN_NAME,DATA_TYPE,CHARACTER_MAXIMUM_LENGTH,NUMERIC_PRECISION,NUMERIC_SCALE,IS_NULLABLE,COLUMN_COMMENT,COLUMN_KEY FROM information_schema.columns where table_name='" + tableName + "'";
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql);
        String columnName = null;// 列名
        String columnType = null;// 类型
        int dataLength = 0; // 列的数据类型的字节长度
        int dataPrecision = 0;//数字类型的实际长度
        int dataScale = 0;// 小数位
        String nullAble = null;
        String comments = null;
        String primaryKey = null;
        for (Map<String, Object> map : mapList) {
            //设置字段名称
            Object column_name = map.get("COLUMN_NAME");
            if (null != column_name) {
                columnName = column_name.toString();
            }
            //设置字段类型
            Object data_type = map.get("DATA_TYPE");
            if (null != data_type) {
                columnType = data_type.toString();
            }
            //设置字段长度
            Object character_maximum_length = map.get("CHARACTER_MAXIMUM_LENGTH");
            if (null != character_maximum_length) {
                String s = character_maximum_length.toString();
                dataLength = Integer.parseInt(s);
            }
            //设置字段实际长度
            Object numeric_precision = map.get("NUMERIC_PRECISION");
            if (null != numeric_precision) {
                String s = numeric_precision.toString();
                dataPrecision = Integer.parseInt(s);
            }
            //设置字段小数位
            Object numeric_scale = map.get("NUMERIC_SCALE");
            if (null != numeric_scale) {
                String s = numeric_scale.toString();
                dataScale = Integer.parseInt(s);
            }
            //设置字段是否为空
            Object nullable = map.get("IS_NULLABLE");
            if (null != nullable) {
                nullAble = nullable.toString();
            }
            //设置字段注释
            Object column_comment = map.get("COLUMN_COMMENT");
            if (null != column_comment) {
                comments = column_comment.toString();
            }
            //获取主键
            Object column_key = map.get("COLUMN_KEY");
            if (null != column_key) {
                if ("".equals(primaryKey) || null == primaryKey) {
                    primaryKey = columnName;
                }
            }
            column = new Column(columnName, columnType, dataLength, dataPrecision, dataScale, nullAble, comments, primaryKey);
            columnList.add(column);
        }
        return columnList;
    }

    public static StringBuffer mysqlDDL2OracleDDL(String tableName, List<Column> columnList){
        StringBuffer strBuffer = new StringBuffer();
        strBuffer.append("CREATE TABLE ").append(tableName.toUpperCase()+"( ");
        String primaryKey = "";
        for (Column column : columnList) {
            String columnName = column.getColumnName().toUpperCase();
            String nullAble = column.getNullAble();
            //设置字段名称
            strBuffer.append(columnName+" ");
            //设置字段类型和长度
            strBuffer.append(mysqlType2OracleType(column)+" ");
            //设置字段是否为空
            if ("NO".equalsIgnoreCase(nullAble)){
                strBuffer.append("NOT NULL ENABLE, ");
            }else{
                strBuffer.append(", ");
            }
            if (null != column.getPrimaryKey() && !"".equals(column.getPrimaryKey())) {
                if ("".equals(primaryKey)) {
                    primaryKey = column.getPrimaryKey().toUpperCase();
                }
            }
        }
        //设置主键
        strBuffer.append("PRIMARY KEY ("+primaryKey+") ");
        strBuffer.append(")");
        return strBuffer;
    }

    /**
     * 将mysql数据type转成oracle数据type
     * @Author:  WS-
     * @return:
     * @Date:    2019/5/13  21:18
     * @Description:
     */
    public static String mysqlType2OracleType(Column column){
        String columnType = column.getColumnType();//字段类型
        int dataLength = column.getDataLength();//字段长度
        int dataScale = column.getDataScale();//字段小数位
        int dataPrecision = column.getDataPrecision();//字段类型实际长度
        if ("longtext".equalsIgnoreCase(columnType)){
            return "BLOB";
        }else if ("datatime".equalsIgnoreCase(columnType)){
            return "DATA";
        }else if ("char".equalsIgnoreCase(columnType)){
            return "CHAR";
        }else if ("varchar".equalsIgnoreCase(columnType) ||"text".equalsIgnoreCase(columnType)){
            if (dataScale>0){
                return "NUMBER("+dataPrecision+","+dataScale+")";
            }else{
                if (dataPrecision==0){
                    dataPrecision=dataLength;
                }
                return "VARCHAR2("+dataLength+")";

            }
        }else if ("decimal".equalsIgnoreCase(columnType) || "tinyint".equalsIgnoreCase(columnType) ||"smallint".equalsIgnoreCase(columnType)
        ||"mediumint".equalsIgnoreCase(columnType) || "int".equalsIgnoreCase(columnType) ||"bigint".equalsIgnoreCase(columnType)){
            return "NUMBER";
        }
        return  columnType;
    }
}
