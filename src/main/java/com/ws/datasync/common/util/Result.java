package com.ws.datasync.common.util;

public class Result {
    private  int code;
    private String msg;
    private Object data;

    public Result(int code, String msg, Object data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }
    public Result(){

    }

    public Result addCode(int code) {
        this.code = code;
        return this;
    }

    public Result addMsg(String msg) {
        this.msg=msg;
        return this;
    }

    public Result addData(Object data) {
        this.data = data;
        return this;
    }
    public static Result success(String content) {
        return new Result(0, content, null);
    }

    public static Result success(String content, Object data) {
        return new Result(0, content, data);
    }

    public static Result error(int code, String content) {
        return new Result(code, content, null);
    }

    public static Result error(String content) {
        return new Result(1, content, null);
    }

    public static Result success() {
        return new Result(0, "globals.result.success", null);
    }

    public static Result error() {
        return new Result(1, "globals.result.error", null);
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
