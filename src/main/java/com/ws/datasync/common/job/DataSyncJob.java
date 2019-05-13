package com.ws.datasync.common.job;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import com.ws.datasync.common.util.ConfigUtil;
import com.ws.datasync.common.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * 数据同步job
 *
 * @author zhangle
 */
public class DataSyncJob implements BeanNameAware {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());

    //此部分属性可在spring配置文件中配置
    private DataSource originds;    //源数据库
    private DataSource targetds;    //目标数据库
    private StrategyEnum strategy;    //同步策略 all:全量同步,delta:增量同步
    private String selectSql;        //查询源表记录sql
    private String insertSql;        //插入目标表记录sql
    private String deleteSql;        //删除目标表记录sql
    private String createSql;       //创建表sql
    private String startTime;        //增量查询的开始时间
    private Integer pageSize = 50000; //分页查询多少行记录,默认5万条

    //以下属性内部使用,不能在spring配置文件中配置
    private AtomicBoolean running;    //任务是否在执行中
    private String beanName;        //spring beanname
    @Autowired
    private ConfigUtil configUtil;    //配置文件工具类
    private NamedParameterJdbcTemplate originJdbTemplate;    //源表操作类
    private NamedParameterJdbcTemplate targetJdbTemplate;    //目标表操作类


    public void setCreateSql(String createSql) {
        this.createSql = createSql;
    }

    public String getCreateSql() {
        return createSql;
    }

    private enum StrategyEnum {
        ALL,    //全量查询
        DELTA    //增量查询
    }

    @PostConstruct
    private void init() {
        running = new AtomicBoolean(false);
        originJdbTemplate = new NamedParameterJdbcTemplate(originds);
        targetJdbTemplate = new NamedParameterJdbcTemplate(targetds);
    }

    public void execute() {

        //只有在未执行状态,才能执行任务
        if (!running.get()) {
            running.set(true);
            try {
                if (StrategyEnum.ALL == strategy) {
                    logger.info("{}:正在执行全量任务", beanName);
                    executeStrategyAll();
                } else if (StrategyEnum.DELTA == strategy) {
                    logger.info("{}:正在执行增量任务", beanName);
                    executeStrategyDelta();
                }
                logger.info("{}:任务执行完毕", beanName);
            } catch (Exception e) {
                logger.warn("{}:任务发生异常 {}", beanName, e.getMessage());
            }
            running.set(false);
        } else {
            logger.warn("{}:任务正在执行中,等待下次执行", beanName);
        }
    }

    /**
     * 执行全量同步
     */
    private void executeStrategyAll() {
        //创建目标表
        //createTargetTable();
        //1.先删除目标表记录
        //deleteTargetTableData();

        Map<String, Object> paramMap = paramMap();
        //如果是分页查询,那么设置分页信息
        if (selectSql.indexOf(":startRow") > 0) {
            int startRow = 0;
            while (true) {
                paramMap.put("startRow", startRow);
                if (selectSql.indexOf(":pageSize") > 0) {
                    paramMap.put("pageSize", pageSize);
                }
                if (selectSql.indexOf(":endRow") > 0) {
                    paramMap.put("endRow", startRow + pageSize);
                }
                logger.debug("{}:query params {}", beanName, paramMap.toString());
                List<Map<String, Object>> list = originJdbTemplate.queryForList(selectSql, paramMap);
                insert(list);

                if (list.size() < pageSize) {    //如果结果集行数小于一页总记录数,那么代表已到最后一页,直接退出循环
                    break;
                }
                startRow += pageSize;
            }
        } else {        //否则非分页查询
            //2.查询源表记录
            logger.debug("{}:query params {}", beanName, paramMap.toString());
            List<Map<String, Object>> list = originJdbTemplate.queryForList(selectSql, paramMap);
            //3.插入目标表记录
            insert(list);
        }
    }

    /**
     * 执行增量同步
     */
    private void executeStrategyDelta() {

        Map<String, Object> paramMap = paramMap();
        if (selectSql.indexOf(":startTime") > 0) {
            Date startTime = getStartTime();
            paramMap.put("startTime", startTime);
        }

        //如果是分页查询,那么设置分页信息
        if (selectSql.indexOf(":startRow") > 0) {
            int startRow = 0;
            while (true) {
                paramMap.put("startRow", startRow);
                if (selectSql.indexOf(":pageSize") > 0) {
                    paramMap.put("pageSize", pageSize);
                }
                if (selectSql.indexOf(":endRow") > 0) {
                    paramMap.put("endRow", startRow + pageSize);
                }
                logger.debug("{}:query params {}", beanName, paramMap.toString());
                List<Map<String, Object>> list = originJdbTemplate.queryForList(selectSql, paramMap);
                configUtil.setProperty(beanName + ".startTime", DateUtil.getDefaultDateTimeStr());    //立即记录此刻时间点,用于保存到配置文件
                insert(list);

                if (list.size() < pageSize) {    //如果结果集行数小于一页总记录数,那么代表已到最后一页,直接退出循环
                    break;
                }
                startRow += pageSize;
            }
        } else {        //否则非分页查询
            logger.debug("{}:query params {}", beanName, paramMap.toString());
            List<Map<String, Object>> list = originJdbTemplate.queryForList(selectSql, paramMap);
            configUtil.setProperty(beanName + ".startTime", DateUtil.getDefaultDateTimeStr());        //立即记录此刻时间点,用于保存到配置文件
            insert(list);
        }

        //保存当前时间到配置文件
        configUtil.save();
    }

    /**
     * 清空目标表记录
     */
    private void deleteTargetTableData() {
        int num = targetJdbTemplate.update(deleteSql, paramMap());
        logger.debug("{}:清空目标表{}条记录", beanName, num);
    }

    /**
     * @Author: WS-
     * @return:
     * @Date: 2019/4/25  11:53
     * @Description:
     */
    private void createTargetTable() {
        int num = originJdbTemplate.update(createSql, paramMap());
        logger.debug("{}:新建表{}", beanName, num);
    }

    /**
     * 插入目标表记录
     *
     * @param list
     */
    private void insert(List<Map<String, Object>> list) {
        if (list == null || list.isEmpty()) return;

        Map<String, Object>[] records = new Map[list.size()];
        records = list.toArray(records);
        targetJdbTemplate.batchUpdate(insertSql, records);
        logger.debug("{}:新增{}条记录", beanName, list.size());
    }

    private Date getStartTime() {

        //优先获取配置文件中的startTime参数;如果获取不到,再获取bean配置的startTime参数;如果都没配置,那么返回null
        String value = configUtil.getProperty(beanName + ".startTime");
        if (value == null) {
            value = this.startTime;
        }
        if (value == null) {
            return null;
        }

        try {
            Date startTime = DateUtil.parse(value, "yyyy-MM-dd HH:mm:ss");
            return startTime;
        } catch (Exception e) {
            logger.error("解析startTime为Date对象时出错 startTime={}", value);
            return null;
        }
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    private HashMap<String, Object> paramMap() {
        return new HashMap<String, Object>();
    }

    public DataSource getOriginds() {
        return originds;
    }

    public DataSource getTargetds() {
        return targetds;
    }

    public void setOriginds(DataSource originds) {
        this.originds = originds;
    }

    public void setTargetds(DataSource targetds) {
        this.targetds = targetds;
    }

    public StrategyEnum getStrategy() {
        return strategy;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public String getInsertSql() {
        return insertSql;
    }

    public void setStrategy(String strategy) {
        this.strategy = StrategyEnum.valueOf(strategy.toUpperCase());
    }

    public void setSelectSql(String selectSql) {
        if (isEmpty(selectSql)) throw new IllegalArgumentException("selectSql is empty");
        this.selectSql = selectSql;
    }

    public void setInsertSql(String insertSql) {
        if (isEmpty(insertSql)) throw new IllegalArgumentException("insertSql is empty");
        this.insertSql = insertSql;
    }

    public String getDeleteSql() {
        return deleteSql;
    }

    public void setDeleteSql(String deleteSql) {
        if (isEmpty(deleteSql)) throw new IllegalArgumentException("deleteSql is empty");
        this.deleteSql = deleteSql;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public void setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    private static boolean isEmpty(String str) {
        return str == null || str.trim().length() == 0;
    }
}
