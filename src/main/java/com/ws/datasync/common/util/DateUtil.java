package com.ws.datasync.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 日期处理工具类
 *
 * @author zhanngle
 */
public class DateUtil {

    /**
     * 获取当前时间的字符形式
     *
     * @param format
     * @return
     */
    public static String getCurrentDateStr(String format) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(new Date());
    }

    /**
     * 获取默认的当前日期字符串
     *
     * @return
     */
    public static String getDefaultDateStr() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return dateFormat.format(new Date());
    }

    /**
     * 获取默认的当前日期与时间字符串
     *
     * @return
     */
    public static String getDefaultDateTimeStr() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return dateFormat.format(new Date());
    }

    /**
     * 格式化日期
     *
     * @param date
     * @param pattern
     * @return
     */
    public static String format(Date date, String pattern) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        return dateFormat.format(date);
    }

    /**
     * 解析日期
     *
     * @param dateStr
     * @param pattern
     * @return
     */
    public static Date parse(String dateStr, String pattern) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
        try {
            return dateFormat.parse(dateStr);
        } catch (ParseException e) {
            throw new RuntimeException("解析日期出错! dateStr=" + dateStr, e);
        }
    }

    /**
     * 获取指定月份的最后一天
     *
     * @param year  年份
     * @param month 月份,从1开始
     * @return
     */
    public static int getLastDayInMonth(int year, int month) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.MONTH, month - 1);
        cal.set(Calendar.YEAR, year);
        return cal.getActualMaximum(Calendar.DATE);
    }
    /**
     * @Author:  WS-
     * @return:
     * @Date:    2019/4/28  19:03
     * @Description:获取当前时间的毫秒值
     */
    public static long getCurrentTime(){
        long currentTime = System.currentTimeMillis()/1000;
        return currentTime;
    }
}
