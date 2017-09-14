package fastweb.udap.web.action;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.ServletActionContext;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.ImpalaClientFactory;

/**
 * 
 * @ClassName: CdnlogErrorStatusQueryAction
 * @author LiFuqiang
 * @date 2016年3月15日 上午10:04:00
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
@Namespace("/")
@Action(value = "cdnlogdomaindistributionaction", results = {
        @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties",
                "message" }),
        @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties",
                "message" }) })
public class CdnlogDomainDistributionAction extends ActionSupport {

    private static final long serialVersionUID = 1592533774413104507L;
    private static final String TYPE_CS = "cs";
    private static final String TYPE_COUNT = "count";
    private static final String TYPE_ALL = "all";
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static int QUERYMAX = 5;
    public static DecimalFormat decimalFormat = new DecimalFormat("0.00");
    private static AtomicInteger queryCnt = new AtomicInteger(QUERYMAX);

    public static final String table = "cdnlog_flow_distribution";
    private static AtomicInteger visitCount = new AtomicInteger(0);

    private final Log log = LogFactory.getLog(getClass());

    /* 错误信息 */
    private StringBuilder message;

    /*
     * type为cs，表明统计流量；type为count，表明统计请求数；type为all，表明流量和请求数都统计
     */
    private String type = TYPE_CS;

    private String userid;// 用户数字代码

    private String domain; //
    private String province; // 省份数字代码
    private String isp; // 运营商数字代码
    private String time; // 时间，精确到天，例如:20170301，必须是8位数的时间表示

    private String[] column;

    private JSONArray result = new JSONArray();

    private JSONObject total = new JSONObject();

    private HttpServletRequest request = null;

    private int limit = 10000;

    @Override
    public String execute() {

        if (this.message != null && this.message.length() != 0) {
            return ActionSupport.ERROR;
        }
        log.info("Query client ip: " + ServletActionContext.getRequest().getRemoteAddr() + ":");
        log.info("query url: " + ServletActionContext.getRequest().getRequestURL() + "?"
                + ServletActionContext.getRequest().getQueryString());
        // String[] columns = { "domain", "userid" };
        // this.column = columns;
        // this.time = "20170301";
        // this.isp = "11";
        // this.userid = "440";
        // this.type = TYPE_CS;
        // 查询开始时间
        long startQuery = System.currentTimeMillis();

        if (queryCnt.get() == 0) {
            log.info("full");
            return ActionSupport.ERROR;
        }
        queryCnt.getAndDecrement();
        log.info("current queryCnt is : " + queryCnt.get());
        String sql = getSqlString();

        log.info(sql);

        sqlExecuteAndJson(sql);
        log.info(this.result);
        queryCnt.incrementAndGet();
        // 查询结束时间
        long endQuery = System.currentTimeMillis();
        // 打印查询结束时间
        log.info(("sql query total time(s) " + (endQuery - startQuery) / (float) 1000));
        return ActionSupport.SUCCESS;
    }

    private String getSqlString() {
        StringBuilder sql = new StringBuilder("select ");

        if (this.column != null) {
            for (String col : this.column) {
                sql.append(" " + col + ", ");
            }
        }

        if (this.type.equals(TYPE_CS)) {
            sql.append(" sum(sum_cs) as sum_cs ");
        }
        else if (this.type.equals(TYPE_COUNT)) {
            sql.append(" sum(count) as count ");
        }
        else if (this.type.equals(TYPE_ALL)) {
            sql.append(" sum(sum_cs) as sum_cs,sum(count) as count ");
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String dayy = sdf.format(new Date(Long.valueOf(this.time) * 1000));
        String month = dayy.substring(0, 6);
        String day = dayy.substring(6, 8);

        sql.append("from " + table + " where ");
        sql.append(" month_=" + month);
        sql.append(" and day_=" + day);

        if (this.userid != null) {
            sql.append(" and userid=" + this.userid);
        }

        if (this.domain != null) {
            sql.append(" and domain='" + this.domain + "'");
        }

        if (this.province != null) {
            String[] prvs = this.province.split(",");
            StringBuilder prvCond = new StringBuilder(" province in (");
            for (String prv : prvs) {
                prvCond.append(prv + ",");
            }
            prvCond.deleteCharAt(prvCond.length() - 1);
            sql.append(" and " + prvCond.toString() + ")");

            // sql.append(" and province=" + this.province);
        }

        if (this.isp != null) {
            String[] isps = this.isp.split(",");
            StringBuilder ispCond = new StringBuilder(" isp in (");
            for (String isp : isps) {
                ispCond.append(isp + ",");
            }
            ispCond.deleteCharAt(ispCond.length() - 1);
            sql.append(" and " + ispCond.toString() + ")");
            // System.out.println();
            // sql.append(" and isp=" + this.isp);
        }

        if (this.column != null) {
            for (int i = 0; i < this.column.length; i++) {
                if (i == 0) {
                    sql.append(" group by ");
                }
                sql.append(this.column[i] + ",");
            }
            sql.deleteCharAt(sql.length() - 1);
        }

        if (this.type.equals(TYPE_CS)) {
            sql.append(" order by sum_cs desc ");
        }
        else if (this.type.equals(TYPE_COUNT)) {
            sql.append(" order by count desc ");
        }
        else if (this.type.equals(TYPE_ALL)) {
            sql.append(" order by sum_cs desc ");
        }

        sql.append(" limit " + this.limit);
        return sql.toString();
    }

    @Override
    public void validate() {

        super.validate();
        if (this.time == null) {
            this.message = new StringBuilder();
            this.message.append("time can not empty!");
        }
        log.info("visit number is :" + visitCount.incrementAndGet());
    }

    /**
     * 
     * @Title: sqlExecuteAndJson
     * @param sql
     *            String
     * @throws
     * @Description:执行sql语句，生成jsonArray，然后将结果写入到缓存文件中。
     */
    private JSONObject sqlExecuteAndJson(String sql) {
        log.info("start execute sql:" + sql);
        Connection conn = null;
        ResultSet rs = null;
        Statement stmt = null;
        List<JSONObject> result = new ArrayList<JSONObject>();
        long csTotal = 0l;
        long countTotal = 0l;
        try {
            Class.forName(driverName);
            conn = ImpalaClientFactory.createClient();
            stmt = conn.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData md = rs.getMetaData();
            int num = md.getColumnCount();

            while (rs.next()) {
                JSONObject json = new JSONObject();
                for (int i = 1; i <= num; i++) {
                    String columnName = md.getColumnName(i);
                    String value = rs.getString(i);
                    json.put(columnName, value);
                    if (columnName.equals("sum_cs")) {
                        csTotal += Long.valueOf(value);

                    }
                    if (columnName.equals("count")) {
                        countTotal += Long.valueOf(value);
                    }
                }
                result.add(json);
            }
            if (csTotal != 0) {
                this.total.put("cs_total", (double) csTotal / 1024 / 1024);
            }
            if (countTotal != 0) {
                this.total.put("count_total", countTotal);
            }
        }
        catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            }
            catch (SQLException e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        }

        for (JSONObject json : result) {
            JSONObject json2 = new JSONObject();
            for (Object key : json.keySet()) {
                double countPercent = 0.0f;
                double csPercent = 0.0f;
                String columnName = (String) key;
                String value = json.getString(columnName);
                if (columnName.equals("sum_cs")) {
                    // json2.put(columnName,
                    // decimalFormat.format(Double.valueOf(value) / 1024 /
                    // 1024));
                    json2.put(columnName, Double.valueOf(value) / 1024 / 1024);
                }
                else if (columnName.equals("count")) {
                    json2.put(columnName, Long.valueOf(value));
                }
                else {
                    json2.put(columnName, value);
                }
                if (columnName.equals("sum_cs")) {
                    csPercent = Double.valueOf(value) / csTotal * 100;
                    json2.put("cs_percent", csPercent);
                }
                if (columnName.equals("count")) {
                    countPercent = Double.valueOf(value) / countTotal * 100;
                    json2.put("count_percent", countPercent);
                }
            }
            this.result.add(json2);
        }
        return null;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setColumn(String[] column) {
        this.column = column;
    }

    public void setUserid(String userid) {
        this.userid = userid;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public JSONArray getResult() {
        return result;
    }

    public void setResult(JSONArray result) {
        this.result = result;
    }

    public String getMessage() {
        return message.toString();
    }

    public JSONObject getTotal() {
        return total;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public static void main(String[] args) {
        new CdnlogDomainDistributionAction().execute();
        long a = 246298813629672l;
        long c = 1048577l;
        String b = "246298813629672";
        System.out.println(a / (double) 1024 / 1024);
        System.out.println(Double.valueOf(b) / 1024 / 1024);
        System.out.println(c / 1024 / 1024);
        System.out.println(a / 1024 / 1024);
    }

}
