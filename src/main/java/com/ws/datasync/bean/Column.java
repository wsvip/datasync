package com.ws.datasync.bean;

import java.io.Serializable;

public class Column implements Serializable {
    private String columnName;// 列名
    private String columnType;// 类型
    private Integer dataLength; // 列的数据类型的字节长度
    private Integer dataPrecision;//数字类型的实际长度
    private Integer dataScale; // 小数位
    private String nullAble;//是否可以为空，Y或N
    private String comments;// 列注释
    private String primaryKey;//主键列

    public Column(String columnName, String columnType, int dataLength, int dataPrecision, int dataScale, String nullAble, String comments, String primaryKey) {
        this.columnName = columnName;
        this.columnType = columnType;
        this.dataLength = dataLength;
        this.dataPrecision = dataPrecision;
        this.dataScale = dataScale;
        this.nullAble = nullAble;
        this.comments = comments;
        this.primaryKey = primaryKey;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }

    public int getDataLength() {
        return dataLength;
    }

    public void setDataLength(int dataLength) {
        this.dataLength = dataLength;
    }

    public int getDataPrecision() {
        return dataPrecision;
    }

    public void setDataPrecision(int dataPrecision) {
        this.dataPrecision = dataPrecision;
    }

    public int getDataScale() {
        return dataScale;
    }

    public void setDataScale(int dataScale) {
        this.dataScale = dataScale;
    }

    public String getNullAble() {
        return nullAble;
    }

    public void setNullAble(String nullAble) {
        this.nullAble = nullAble;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public String getPrimaryKey() {
        return primaryKey;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }
}
