package com.ws.datasync.controller;

import com.ws.datasync.bean.Column;
import com.ws.datasync.common.util.DateUtil;
import com.ws.datasync.common.util.Mysql2OracleUtil;
import com.ws.datasync.common.util.Oracle2MysqlUtil;
import com.ws.datasync.common.util.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/datasync")
public class IndexController implements BeanNameAware {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    @Autowired
    @Qualifier("primaryJdbcTemplate")
    protected JdbcTemplate jdbcTemplate1; //本地数据库

    @Autowired
    @Qualifier("secondaryJdbcTemplate")
    protected JdbcTemplate jdbcTemplate2;//源数据库

    @Value("${spring.datasource.primary.name}")
    public String primaryName;   //本地数据库名

    @Value("${spring.datasource.secondary.name}")
    public String secondaryName;  //源数据库名

    @Value("${spring.datasource.primary.type}")
    public String primaryDBType;   //本地数据库类型

    @Value("${spring.datasource.secondary.type}")
    public String secondaryDBType;  //源数据库类型


    private String doubleOracle = "doubleOracle";//两个数据库都是oracle数据库
    private String doubleMysql = "doubleMysql";//两个数据库都是mysql数据库
    private String oracleAndMysql = "oracleAndMysql";//primaryDB是oralce，secondaryDB是mysql
    private String mysqlAndOracle = "mysqlAndOracle";//primaryDB是mysql，secondaryDB是oralce

    //成功时0
    private int SYNC_TABLE_SUCCESS_CODE=0;
    //备份表时表名过长
    private int BK_TABLE_TOOLONG_CODE=1;
    //新建表时报错
    private int NEW_TABLE_ERROR_CODE=2;
    //插入数据时报错
    private int INSERT_TABLE_ERROR_CODE=3;



    private String beanName;//获取当前类名称

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    @RequestMapping("/index")
    public String index(HttpServletRequest request) {
        System.err.println("成功访问！");
        String sql = null;
        if ("oracle".equalsIgnoreCase(secondaryDBType)) {
            sql = "SELECT TABLE_NAME FROM USER_TAB_COMMENTS";
        } else if ("mysql".equalsIgnoreCase(secondaryDBType)) {
            sql = "show tables";
        } else {

        }
        ArrayList<String> tableList = screenData(sql);
        request.setAttribute("tables", tableList);
        return "index";
    }

    @RequestMapping("/seach")
    @ResponseBody
    public Object seach(String condition, HttpServletRequest request) {
        String sql = null;
        if ("oracle".equalsIgnoreCase(secondaryDBType)) {
            sql = "SELECT TABLE_NAME FROM USER_TAB_COMMENTS";
            if (null != condition) {
                condition = condition.toUpperCase();
                sql += " WHERE TABLE_NAME LIKE '%" + condition + "%'";
            }
        } else if ("mysql".equalsIgnoreCase(secondaryDBType)) {
            sql = "select table_name from (select table_name from information_schema.tables where table_schema='" + secondaryName + "') t";
            if (null != condition) {
                sql += " where t.table_name like '%" + condition + "%'";
            }
        } else {

        }
        ArrayList<String> tableList = screenData(sql);
        request.setAttribute("seachTableList", tableList);
        return Result.success("成功", tableList);
    }

    /**
     * 筛选数据
     *
     * @Author: WS-
     * @return:
     * @Date: 2019/5/7  16:48
     * @Description:
     */
    private ArrayList<String> screenData(String sql) {
        List<Map<String, Object>> maps = jdbcTemplate2.queryForList(sql);
        ArrayList<String> tableList = new ArrayList<>();
        for (Map<String, Object> map : maps) {
            for (Object value : map.values()) {
                tableList.add(value.toString());
            }
        }
        return tableList;
    }

    @RequestMapping(value = "/doSyncData", method = RequestMethod.POST)
    @ResponseBody
    public Result doSyncData(@RequestParam("tableName") String tableName) {
        try {
            //查询本地数据库中是否存在该表，不存在则直接新建表，存在则备份
            int count = checkTable(tableName);
            if (count == 0) {
                log.info("{}:不存在表：{}", beanName, tableName);
                //本地数据库不存在该表，直接新建表
                log.info("{}:开始新建表:{}", beanName, tableName);
                try {
                    createTable(tableName);
                } catch (Exception e) {
                    e.printStackTrace();
                    return Result.error(NEW_TABLE_ERROR_CODE,"重建表失败");
                }
            } else {
                //1、修改目标表名（备份）
                log.info("{}:存在表:{}", beanName, tableName);
                log.info("{}:开始备份表:{}", beanName, tableName);
                try {
                    backUpTable(tableName);
                } catch (Exception e) {
                    e.printStackTrace();
                    return Result.error(BK_TABLE_TOOLONG_CODE,"表名过长，备份表失败");
                }
                //2、新建表(获取建表DDL语句)
                log.info("{}:开始重建表:{}", beanName, tableName);
                try {
                    createTable(tableName);
                } catch (Exception e) {
                    e.printStackTrace();
                    return Result.error(NEW_TABLE_ERROR_CODE,"重建表失败");
                }
            }
            //3、插入数据
            log.info("{}:开始更新，往表:{}中插入数据", beanName, tableName);
            int dataCount = insertDataToTable(tableName);
            log.info("{}:同步成功，往表:{}插入了{}条数据", beanName, tableName, dataCount);
            //return ResultUtil.success("同步成功");
            return Result.success("同步成功");
        } catch (Exception e) {
            e.printStackTrace();
            log.info("{}:同步:{} 表失败", beanName, tableName);
            //return ResultUtil.error("同步失败");
            return Result.error("同步失败");
        }
    }

    /**
     * 创建表
     *
     * @Author: WS-
     * @return:
     * @Date: 2019/4/26  15:41
     * @Description:
     */
    private void createTable(String tableName) {
        String getDDLSql = null;
        //获取建表语句
        if ("oracle".equalsIgnoreCase(secondaryDBType)) {
            getDDLSql = "SELECT DBMS_METADATA.GET_DDL('TABLE','" + tableName + "','" + secondaryName + "') AS  DDL FROM DUAL";
        } else if ("mysql".equalsIgnoreCase(secondaryDBType)) {
            getDDLSql = "show create table " + tableName;
        } else {

        }
        List<Map<String, Object>> maps = jdbcTemplate2.queryForList(getDDLSql);
        String ddl = null;
        String createTableSql=null;
        String DBType = checkDBType(primaryDBType, secondaryDBType);

        //两个数据库都是oracle
        if (DBType.equalsIgnoreCase(doubleOracle)) {
            ddl = maps.get(0).get("DDL").toString();
            //将源数据库中数据名改为本地数据库名
            createTableSql= ddl.replaceAll(secondaryName, primaryName);
        }
        //两个数据库都是mysql
        else if (DBType.equalsIgnoreCase(doubleMysql)) {
            ddl = maps.get(0).get("Create Table").toString();
            //将源数据库中数据名改为本地数据库名
            createTableSql= ddl.replaceAll(secondaryName, primaryName);
        }
        //本地数据库是oracle，源数据库是mysql
        else if (DBType.equalsIgnoreCase(oracleAndMysql)) {
            ArrayList<Column> mysqlColumn = Mysql2OracleUtil.getMysqlColumn(tableName, jdbcTemplate2);
            createTableSql=Mysql2OracleUtil.mysqlDDL2OracleDDL(tableName,mysqlColumn).toString();
        }
        //本地数据库是mysql，源数据库是oracle
        else if (DBType.equalsIgnoreCase(mysqlAndOracle)) {
            //获取源数据库oracle的字段和字段长度等信息
            List<Column> oracleColumn = Oracle2MysqlUtil.getOracleColumn(tableName, jdbcTemplate2);
            //将oracle建表语句改成mysql建表语句
            createTableSql = Oracle2MysqlUtil.oracleDDL2Mysql(tableName, oracleColumn).toString();
        }else{
            log.info("{}:数据库类型无法判断！",beanName);
        }

        jdbcTemplate1.execute(createTableSql);
        log.info("{}:新建表:{}成功", beanName, tableName);
    }

    /**
     * 备份表
     *
     * @Author: WS-
     * @return:
     * @Date: 2019/4/26  15:42
     * @Description:
     */
    private void backUpTable(String tableName) throws Exception {
        //获取当前时间，并修改成yyyy-MM_dd_HH_mm_ss模式
        String defaultDateStr = DateUtil.getDefaultDateTimeStr();
        long currentTime = DateUtil.getCurrentTime();
        //以时间毫秒值为结尾进行表备份
        String newName = tableName + "_BK_" + currentTime;
        //防止oracle表名超过30位报错
        if ("oracle".equalsIgnoreCase(primaryDBType)){
            if (tableName.length()>30){
                throw new Exception("表名过长，请手动备份！");
            }
            String tempName=tableName+"_BK_";
            String bkTime=currentTime+"";
            if (newName.length()>30){
                int bkLength = 30 - tempName.length();
                String end = bkTime.substring(bkLength);
                newName=tableName+end;
            }
        }
        String renameTableSql = "ALTER TABLE " + tableName + " RENAME TO " + newName;
        jdbcTemplate1.execute(renameTableSql);
        log.info("{}:备份表:{}成功，新表名为:{}", beanName, tableName, newName);
    }

    /**
     * 将数据插入到表中
     *
     * @Author: WS-
     * @return:
     * @Date: 2019/4/26  15:44
     * @Description:
     */
    private int insertDataToTable(String tableName) {
        String getDataListSql = "SELECT * FROM " + tableName;
        if ("mysql".equalsIgnoreCase(secondaryDBType)){
            getDataListSql=getDataListSql.toLowerCase();
        }
        List<Map<String, Object>> list = jdbcTemplate2.queryForList(getDataListSql);
        //查询表字段数量
        String fildCountSql = null;
        if ("oracle".equalsIgnoreCase(secondaryDBType)) {
            fildCountSql = "SELECT A.COLUMN_NAME FROM USER_TAB_COLUMNS A WHERE TABLE_NAME ='" + tableName.toUpperCase() + "'";
        } else if ("mysql".equalsIgnoreCase(secondaryDBType)) {
            fildCountSql = "select COLUMN_NAME from information_schema.columns where table_name ='" + tableName + "'";
        } else {

        }
        List<Map<String, Object>> fildCountList = jdbcTemplate2.queryForList(fildCountSql);
        int size = fildCountList.size();
        String temp = "";
        for (int i = 0; i < size; i++) {
            temp += "?,";
        }
        temp = temp.substring(0, temp.lastIndexOf(","));
        String insertSql = "INSERT INTO " + tableName + " VALUES(" + temp + ")";
        int count = 0;
        for (Map<String, Object> map : list) {
            ArrayList<Object> tempList = new ArrayList<>();
            for (Object value : map.values()) {
                tempList.add(value);
            }
            Object[] obj = tempList.toArray();
            count++;
            int update = jdbcTemplate1.update(insertSql, obj);
        }
        return count;
    }

    /**
     * @Author: WS-
     * @return:
     * @Date: 2019/4/26  15:54
     * @Description: 校验表是否存在
     */
    private int checkTable(String tableName) {
        String checkTableSql = null;
        if ("oracle".equalsIgnoreCase(primaryDBType)) {
            checkTableSql = "SELECT COUNT(*) AS C FROM USER_TABLES WHERE TABLE_NAME = '" + tableName.toUpperCase() + "'";
        } else if ("mysql".equalsIgnoreCase(primaryDBType)) {
            checkTableSql = "select count(*) as C from (select table_name from information_schema.tables where table_schema='" + primaryName + "') t where t.table_name = '" + tableName + "'";
        } else {

        }
        List<Map<String, Object>> listMap = jdbcTemplate1.queryForList(checkTableSql);
        String c = listMap.get(0).get("C").toString();
        Integer count = Integer.valueOf(c);
        return count;
    }

    @RequestMapping(value = "/test", method = RequestMethod.GET)
    public void test() {
        //List<Column> test = Oracle2MysqlUtil.getOracleColumn("TEST", jdbcTemplate2);
        ArrayList<Column> two = Mysql2OracleUtil.getMysqlColumn("two", jdbcTemplate2);
    }


    /**
     * 确认本地数据库和源数据库
     *
     * @param primaryDBType   本地数据库
     * @param secondaryDBType 源数据库
     * @Author: WS-
     * @return:
     * @Date: 2019/5/13  14:49
     * @Description:
     */
    private String checkDBType(String primaryDBType, String secondaryDBType) {
        //两个数据库都是oracle
        if ("oracle".equalsIgnoreCase(primaryDBType) && "oracle".equalsIgnoreCase(secondaryDBType)) {
            return "doubleOracleDB";
        }
        //两个数据库都是mysql
        else if ("mysql".equalsIgnoreCase(primaryDBType) && "mysql".equalsIgnoreCase(secondaryDBType)) {
            return "doubleMysqlDB";
        }
        //primaryDB是oracle，secondaryDB是mysql
        else if ("oracle".equalsIgnoreCase(primaryDBType) && "mysql".equalsIgnoreCase(secondaryDBType)) {
            return "oracleAndMysql";
        }
        //primaryDB是mysql，secondaryDB是oracle
        else if ("mysql".equalsIgnoreCase(primaryDBType) && "oracle".equalsIgnoreCase(secondaryDBType)) {
            return "mysqlAndOracle";
        } else {
            return "error";
        }
    }
}
