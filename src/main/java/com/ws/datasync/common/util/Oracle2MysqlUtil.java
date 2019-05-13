package com.ws.datasync.common.util;

import com.ws.datasync.bean.Column;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Oracle2MysqlUtil {

    /**
     * 获取oracle表：字段名称、字段类型、字段长度、数字类型的实际长度、小数位、是否可以为空-Y或N、列注释、主键列
     *
     * @param tableName
     * @Author: WS-
     * @return:
     * @Date: 2019/5/9  15:14
     * @Description:
     */
    public static List<Column> getOracleColumn(String tableName, JdbcTemplate jdbcTemplate) {
        ArrayList<Column> columnlist = new ArrayList<>();
        Column column = null;
        String sql = "SELECT T1.COLUMN_NAME,T1.DATA_TYPE ,T1.DATA_LENGTH,T1.DATA_PRECISION,T1.DATA_SCALE,T1.NULLABLE,T2.COMMENTS,T5.COLUMN_NAME PRIMARYKEY " +
                " FROM ALL_TAB_COLUMNS T1 LEFT JOIN USER_COL_COMMENTS T2 ON T1.TABLE_NAME=T2.TABLE_NAME AND T1.COLUMN_NAME=T2.COLUMN_NAME " +
                " LEFT JOIN (SELECT T3.TABLE_NAME,T3.COLUMN_NAME FROM USER_CONS_COLUMNS T3, USER_CONSTRAINTS T4 " +
                " WHERE T3.CONSTRAINT_NAME = T4.CONSTRAINT_NAME AND T4.CONSTRAINT_TYPE = 'P') T5 ON T1.TABLE_NAME=T5.TABLE_NAME AND T1.COLUMN_NAME=T5.COLUMN_NAME " +
                " WHERE T1.TABLE_NAME='" + tableName + "'";
        List<Map<String, Object>> mapList = jdbcTemplate.queryForList(sql);
        String columnName = null;
        String columnType = null;
        int dataLength = 0;
        int dataPrecision = 0;
        int dataScale = 0;
        String nullAble = null;
        String comments = null;
        String primaryKey = null;
        for (Map<String, Object> map : mapList) {
            try {
                Object column_name = map.get("COLUMN_NAME");
                if (null != column_name) {
                    columnName = column_name.toString();
                }
                Object data_type = map.get("DATA_TYPE");
                if (null != data_type) {
                    columnType = data_type.toString();
                }
                Object nullable = map.get("NULLABLE");
                if (null != nullable) {
                    nullAble = nullable.toString();
                }
                Object comments1 = map.get("COMMENTS");
                if (null != comments1) {
                    String s = comments1.toString();
                    comments = s;
                }
                Object primarykey = map.get("PRIMARYKEY");
                if (null != primarykey) {
                    primaryKey = primarykey.toString();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                Object data_length = map.get("DATA_LENGTH");
                if (null != data_length) {
                    String s = data_length.toString();
                    dataLength = Integer.parseInt(s);
                }
            } catch (Exception e) {
                dataLength = 0;
                e.printStackTrace();
            }
            try {
                Object data_precision = map.get("DATA_PRECISION");
                if (null != data_precision) {
                    String s = data_precision.toString();
                    dataPrecision = Integer.parseInt(s);
                }
            } catch (Exception e) {
                dataPrecision = 0;
                e.printStackTrace();
            }
            try {
                Object data_scale = map.get("DATA_SCALE");
                if (null != data_scale) {
                    String s = data_scale.toString();
                    dataScale = Integer.parseInt(s);
                }
            } catch (Exception e) {
                dataScale = 0;
                e.printStackTrace();
            }
            column = new Column(columnName, columnType, dataLength, dataPrecision, dataScale, nullAble, comments, primaryKey);
            columnlist.add(column);
        }
        return columnlist;
    }
    /**
     * 将oracle建表语句改为mysql建表语句
     * @Author:  WS-
     * @return:
     * @Date:    2019/5/13  14:22
     * @Description:
     */
    public static StringBuffer oracleDDL2Mysql(String tableName, List<Column> columnList) {
        StringBuffer strBuffer = new StringBuffer();
        //拼接建表语句
        strBuffer.append("create table ").append(tableName.toLowerCase()).append("(");
        String primaryKey = "";
        for (Column column : columnList) {
            String columnName = column.getColumnName();//字段名称
            String nullAble = column.getNullAble();//字段是否允许为空
            //设置字段名称
            strBuffer.append(columnName + " ");
            //设置字段类型和长度
            strBuffer.append(oracleType2MysqlType(column) + " ");
            //设置字段是否为空
            if ("N".equalsIgnoreCase(nullAble)) {
                strBuffer.append(" NOT NULL,");
            } else {
                strBuffer.append(" DEFAULT NULL,");
            }
            //获取主键
            if (null != column.getPrimaryKey() && !"".equals(column.getPrimaryKey())) {
                if ("".equals(primaryKey)) {
                    primaryKey = column.getPrimaryKey();
                }
            }
        }
        //设置主键
        strBuffer.append("PRIMARY KEY ("+primaryKey+") ");
        //设置编码
        strBuffer.append(") ENGINE=InnoDB DEFAULT CHARSET=utf8");

        return strBuffer;
    }

    /**
     * 将oracle类型转换成mysql类型
     * @Author: WS-
     * @return:
     * @Date: 2019/5/13  10:43
     * @Description:
     */
    public static String oracleType2MysqlType(Column column) {
        String columnType = column.getColumnType();//字段类型
        int dataLength = column.getDataLength();//字段长度
        int dataScale = column.getDataScale();//字段小数位
        int dataPrecision = column.getDataPrecision();//字段类型实际长度
        if ("BLOB".equalsIgnoreCase(columnType) || "CLOB".equalsIgnoreCase(columnType) || "RAW".equalsIgnoreCase(columnType)) {
            return "longtext";
        } else if ("DATE".equalsIgnoreCase(columnType)) {
            return "datetime";
        } else if ("CHAR".equalsIgnoreCase(columnType)) {
            return "char";
        } else if ("VARCHAR2".equalsIgnoreCase(columnType)) {
            if (dataLength >= 2000) {
                return "text";
            } else {
                return "varchar(" + dataLength + ")";
            }
        } else if ("NUMBER".equalsIgnoreCase(columnType)) {
            if (dataScale > 0) {
                return "decimal(" + dataPrecision + "," + dataScale + ")";
            } else {
                if (dataPrecision == 0) {
                    dataPrecision = dataLength;
                }

                if (dataPrecision <= 3) {
                    return "tinyint(" + dataPrecision + ")";
                } else if (dataPrecision <= 5) {
                    return "smallint(" + dataPrecision + ")";
                } else if (dataPrecision <= 7) {
                    return "mediumint(" + dataPrecision + ")";
                } else if (dataPrecision <= 10) {
                    return "int(" + dataPrecision + ")";
                } else {
                    return "bigint(" + dataPrecision + ")";
                }
            }
        }
        return columnType;
    }
}
