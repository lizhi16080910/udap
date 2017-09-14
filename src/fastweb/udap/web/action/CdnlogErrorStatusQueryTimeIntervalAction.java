package fastweb.udap.web.action;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.ServletActionContext;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.DatetimeUtil;
import fastweb.udap.util.IPUtil;
import fastweb.udap.util.StringUtil;
import fastweb.udap.web.ImpalaClientFactory;

/**
 * 
 * @ClassName: CdnlogErrorStatusQueryAction
 * @author LiFuqiang
 * @date 2016年3月15日 上午10:04:00
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
@Namespace("/")
@Action(value = "cdnlogerrorstatusquerytimeintervalaction", results = {
        @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties",
                "message" }),
        @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties",
                "message" }) })
public class CdnlogErrorStatusQueryTimeIntervalAction extends ActionSupport {

    private static final long serialVersionUID = 1592533774413104507L;
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static int QUERYMAX = 5;
    private static AtomicInteger queryCnt = new AtomicInteger(QUERYMAX);
    // private static AtomicInteger queryCnt = null;
    private static Map<String, String> city = null; // 缓存城市id与城市名称
    private static Map<String, String> isp = null; // 缓存运营商id与运营商名称
    private static Map<String, String> user = null; // 缓存用户id与用户名称
    private static Map<String, String> popName = null; // 缓存机房id与机房名称

    private static Map<String, String> resultCache = Collections.synchronizedMap(new LRUMap(200));
    // private static Map<String, String> resultCache = null;
    public static final String table = "cdnlog_error_status";
    private static String webRootPath = null;
    private static AtomicInteger visitCount = new AtomicInteger(0);
    private static String confPath = null;
    private static String cachePath = null;

    private final Log log = LogFactory.getLog(getClass());

    private String reslt;
    /* 错误信息 */
    private String message;

    private int limit = 0;
    private String[] type;
    private long startTime = 0;
    private long endTime = 0;
    private int interval = 300;
    private String[] clist;
    private String order1;
    private String order2;

    private String usercode_ = null;

    JSONArray result = new JSONArray();
    private long total = 0;
    private String queryInfo;

    private HttpServletRequest request = null;

    @Override
    public String execute() {

        if (!StringUtil.isEmpty(message)) {
            return ActionSupport.ERROR;
        }
        inital();
        log.info(queryInfo);

        // 查询开始时间
        long startQuery = System.currentTimeMillis();

        if (queryCnt.get() == 0) {
            setMessage("full");
            log.info("full");
            return ActionSupport.ERROR;
        }
        queryCnt.getAndDecrement();
        log.info("current queryCnt is : " + queryCnt.get());

        // 首先根据查询参数，判断是否有缓存，如果有缓存，则从缓存读取查询结果，否则生成sql语句，向Impala发送查询请求
        if (ifcache(sortedSQL())) {
            readCache(sortedSQL());
        }
        else {

            String sql = queryString();
            if (sql.equals("error")) {
                // setMessage("error");
                queryCnt.incrementAndGet();
                return ActionSupport.ERROR;
            }
            log.info(sql);
            ifCityIspUseridPopid(); // 判断是否包含prv，isp，以及userid，popid，如果包含，则替换成相应的名称。
            sqlExecuteAndJson(sql);
        }
        queryCnt.incrementAndGet();
        // 查询结束时间
        long endQuery = System.currentTimeMillis();
        // 打印查询结束时间
        log.info(("sql query total time(s) " + (endQuery - startQuery) / (float) 1000));
        return this.reslt;
    }

    @Override
    public void validate() {

        super.validate();

        log.info("visit number is :" + visitCount.incrementAndGet());
        if (this.startTime >= this.endTime) {
            log.error("startTime=" + this.startTime + "; endTime=" + this.endTime);
            log.error("开始时间大于或等于结束时间");
            setMessage("开始时间大于或等于结束时间");
        }
        if ((this.endTime - this.startTime) / (float) 24 / 3600 > 7.0) {
            log.error("startTime=" + this.startTime + "; endTime=" + this.endTime);
            log.error("查询时间超过7天");
            setMessage("查询时间超过7天");
        }

        if (type == null && clist == null) {
            log.error("url:" + this.queryInfo);
            log.error("parameter clist and type can not be both empty!");
            setMessage("parameter clist and type can not be both empty!");
        }

        boolean orderNormal = false;
        if (type != null) {
            for (String str2 : type) {
                if (str2.equals("cs")) {
                    if (order1.contains("sum_cs")) {
                        orderNormal = true;
                    }
                }
                if (str2.equals("count")) {
                    if (order1.contains("count")) {
                        orderNormal = true;
                    }
                }
            }
        }
        else {
            orderNormal = true;
        }
        if (!orderNormal) {
            setMessage("error: order1 parameter and type parameter not coincidence");
            log.error("url:" + this.queryInfo);
            log.error("error: order1 parameter and type parameter not coincidence");
        }

        if (this.type != null && this.clist != null) {
            if (this.limit == 0) {
                setMessage("error: limit parameter can not empty");
                log.error("url:" + this.queryInfo);
                log.error("error: limit parameter can not empty");
            }
        }

    }

    /**
     * 
     * @Title: inital
     * @throws
     * @Description: 实现相关参数的赋值及初始化
     */
    private void inital() {

        this.request = ServletActionContext.getRequest();

        if (webRootPath == null) {
            webRootPath = request.getSession().getServletContext().getRealPath("/");
        }
        if (confPath == null) {
            confPath = webRootPath + "ImpalaToolsConf" + File.separator;
        }

        if (cachePath == null) {
            cachePath = webRootPath + "ImpalaToolsResultCache" + File.separator;
        }
        queryInfo = request.getQueryString();

        log.info("host:" + this.request.getRemoteHost() + "; " + "user:"
                + this.request.getRemoteUser() + "; " + this.request.getRemoteAddr() + ":"
                + this.request.getRemotePort());
    }

    /**
     * 
     * @Title: sqlExecuteAndJson
     * @param sql
     *            String
     * @throws
     * @Description:执行sql语句，生成jsonArray，然后将结果写入到缓存文件中。
     */
    private void sqlExecuteAndJson(String sql) {
        log.info("start execute sql:" + sql);
        Connection conn = null;
        ResultSet rs = null;
        Statement stmt = null;
        try {
            Class.forName(driverName);
            conn = ImpalaClientFactory.createClient();
            stmt = conn.createStatement();
            String fileName = Long.valueOf(System.currentTimeMillis()).toString();
            rs = stmt.executeQuery(sql);
            generateJson(rs); // 生成json

            if (this.result.size() != 0) {
                saveToFile(this.result, fileName); // 写入缓存
                resultCache.put(sortedSQL(), fileName);
                saveResultCacheToFile(cachePath, "index");
            }
            this.reslt = ActionSupport.SUCCESS;
        }
        catch (Exception e) {
            setMessage(e.getMessage());
            this.reslt = ActionSupport.ERROR;
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
    }

    /**
     * 
     * @Title: generateJson
     * @param rs
     *            ResultSet
     * @throws Exception
     * @throws
     * @Description: 根据查询结果，生成jsonArray
     */
    private void generateJson(final ResultSet rs) throws Exception {
        ResultSetMetaData md;
        try {
            md = rs.getMetaData();
            int num = md.getColumnCount();
            while (rs.next()) {

                JSONObject mapOfColValues = new JSONObject();
                // DecimalFormat df = new DecimalFormat("#,###.##");
                this.total++;
                mapOfColValues.put("id", this.total);
                for (int i = 1; i <= num; i++) {
                    if (md.getColumnName(i).toString().equals("sum_cs") && rs.getObject(i) == null) {
                        this.total = 0;
                        this.result.clear();
                        return;
                    }
                    else if (md.getColumnName(i).toString().equals("count")
                            && Long.valueOf(rs.getString(i)) == 0) {
                        this.total = 0;
                        this.result.clear();
                        return;
                    }
                    else {
                        if (md.getColumnName(i).equals("ip")) {
                            mapOfColValues.put(md.getColumnName(i),
                                    IPUtil.int2ip(Long.valueOf(rs.getString(i))));
                        }
                        else {
                            if (rs.getObject(i) == null && num == 1) {
                                this.total = 0;
                                this.result.clear();
                                return;
                            }
                            else if (rs.getObject(i) == null) {
                                mapOfColValues.put(md.getColumnName(i), "-");
                            }
                            else {
                                mapOfColValues.put(md.getColumnName(i), rs.getObject(i));
                            }
                        }
                    }
                }

                result.add(mapOfColValues);
            }
        }
        catch (SQLException e) {
            log.error(e.toString());
            e.printStackTrace();
        }
    }

    /**
     * 
     * @Title: saveToFile
     * @param rs
     *            JSONArray
     * @param fileName
     *            String
     * @throws Exception
     * @throws
     * @Description: 将结果保存到文件中
     */
    private void saveToFile(final JSONArray rs, String fileName) throws Exception {

        FileOutputStream fos = new FileOutputStream(createCSVFile(fileName, cachePath));
        OutputStreamWriter filewriter = new OutputStreamWriter(fos, "UTF-8");

        try {
            for (Object key : rs.getJSONObject(0).keySet()) {
                filewriter.write(key.toString() + ",");
            }
            filewriter.write("\n");
            for (int i = 0; i < rs.size(); i++) {
                JSONObject jsobj = rs.getJSONObject(i);
                for (Object key : jsobj.keySet()) {
                    filewriter.write(jsobj.getString(key.toString()) + ",");
                }
                filewriter.write("\n");
            }
            filewriter.flush();
        }
        catch (Exception e) {
            log.info(e.toString());
            e.printStackTrace();
        }
        finally {
            filewriter.close();
            fos.close();
        }
    }

    /**
     * 
     * @Title: createCSVFile
     * @param fileName
     *            String
     * @param outPutPath
     *            String
     * @return File
     * @throws IOException
     * @throws
     * @Description: 创建缓存文件,后缀为.cache
     */
    private File createCSVFile(String fileName, String outPutPath) throws IOException {
        File file = new File(outPutPath + fileName + ".cache");
        file.getParentFile().mkdir();
        file.createNewFile();
        return file;
    }

    /**
     * 
     * @Title: queryString
     * @return String
     * @throws
     * @Description: 生成查询字符串
     */
    @SuppressWarnings({ "null", "unchecked" })
    private String queryString() {

        try {
            String startTime = DatetimeUtil.timestampToDateStr(this.startTime, "yyyyMMddHHmmss");
            String endTime = DatetimeUtil.timestampToDateStr(this.endTime, "yyyyMMddHHmmss");

            String startMonth = startTime.substring(0, 6);
            String endMonth = endTime.substring(0, 6);

            // 取得查询天
            String startDay = startTime.toString().substring(6, 8);
            String endDay = endTime.toString().substring(6, 8);

            String timeCondition = "";
            String monthCondition = "";
            String dayCondition = "";

            if (startMonth.equals(endMonth)) {
                monthCondition = "month_ = '" + endMonth + "'";
                if (startDay.equals(endDay)) {
                    dayCondition = "day_ = '" + endDay + "'";
                }
                else {
                    dayCondition = " day_ between '" + startDay + "' and '" + endDay + "' ";
                }
            }
            else {
                monthCondition = "month_ in ('" + startMonth + "','" + endMonth + "')";
            }

            timeCondition = " timestmp between '" + startTime + "' and '" + endTime + "'";

            StringBuilder sql = new StringBuilder("select ");
            if (clist != null) {
                for (String str2 : clist) {
                    sql.append(str2 + ",");
                }
            }

            sql.append(getTimeStr(this.interval) + ",");

            if (type != null) {
                for (String str2 : type) {
                    if (str2.equals("cs")) {
                        sql.append("sum(cs) as sum_cs,");
                    }
                    if (str2.equals("count")) {
                        sql.append("count(*) as count,");
                    }
                }
            }

            sql.deleteCharAt(sql.length() - 1);
            sql.append(" from " + table + " where ");
            List<String> condlist = produceConditionList();

            if (this.usercode_ != null) {
                sql.append(this.usercode_ + " and ");
            }
            // sql.append(monthCondition);
            if (!dayCondition.equals("")) {
                sql.append(monthCondition + " and " + dayCondition + " and " + timeCondition);
            }
            else {
                sql.append(monthCondition + " and " + timeCondition);
            }

            if (condlist != null) {
                for (String str : condlist) {
                    sql.append(" and " + str);
                }
            }
            if (type == null) {
                if (this.limit != 0) {
                    sql.append(" limit " + this.limit);
                }
                return sql.toString();
            }
            else {
                if (clist != null) {
                    sql.append(" group by  ");
                    for (String str2 : clist) {
                        sql.append(str2 + ",");
                    }
                    sql.append("time" + ",");
                    sql.deleteCharAt(sql.length() - 1);
                    // sql.append(" order by " + this.order1 + " " +
                    // this.order2);
                    sql.append(" order by time ");
                    // sql.append(" limit " + this.limit);
                }
                else {
                    sql.append(" group by  ");
                    sql.append("time" + ",");
                    sql.deleteCharAt(sql.length() - 1);
                    // sql.append(" order by " + this.order1 + " " +
                    // this.order2);
                    sql.append(" order by time ");
                    // sql.append(" limit " + this.limit);
                }
            }
            return sql.toString();
        }
        catch (Exception e) {
            log.error("url:" + this.queryInfo);
            log.error(e);
            setMessage("error" + e.getMessage());
            return "error";
        }
    }

    /**
     * 
     * @Title: produceConditionList
     * @return List<String>
     * @throws
     * @Description: 生成条件字符串,如果检测到条件字段中包含domain或者userid的话，计算usercode。
     */
    private List<String> produceConditionList() {
        HashMap<String, String> termMap = new HashMap<String, String>();
        termMap.put("1", "=");
        termMap.put("2", "like");

        HashMap<String, String> queryMap = new HashMap<String, String>();
        // 获取查询URL
        List<String> cond1 = new ArrayList<String>();
        try {
            String path = this.request.getRequestURI();
            String actionPath = ".." + path.substring(9);
            if (queryInfo != null && (!queryInfo.equals(""))) {
                actionPath = actionPath + "?" + queryInfo;
            }
            String[] queryList = queryInfo.split("&");
            for (int i = 0; i < queryList.length; i++) {
                if (queryList[i].startsWith("col")) {
                    String[] temp = queryList[i].split("=");
                    queryMap.put(temp[0], temp[1]);
                }
                if (queryList[i].startsWith("term")) {
                    String[] temp = queryList[i].split("=");
                    queryMap.put(temp[0], temp[1]);
                }
                if (queryList[i].startsWith("text")) {
                    String[] temp = queryList[i].split("=");
                    queryMap.put(temp[0], temp[1]);
                }
            }

            for (int i = 1; i <= queryMap.size() / 3; i++) {
                if (queryMap.get("term" + i).equals("2")) {
                    cond1.add(queryMap.get("col" + i) + " like '%" + queryMap.get("text" + i)
                            + "%'");
                }
                else {
                    String col = queryMap.get("col" + i);
                    String term = termMap.get(queryMap.get("term" + i));
                    String text = queryMap.get("text" + i);
                    if (col.equals("domain") && term.equals("=")) {
                        int usercodeT = getUsercode(text);
                        if (usercodeT != -1) {
                            this.usercode_ = "usercode_=" + (usercodeT);
                        }
                    }
                    if (col.equals("userid") && term.equals("=")) {
                        this.usercode_ = "usercode_=" + (computeUsercode(text));
                    }
                    if (col.equals("es") || col.equals("dd")) {
                        cond1.add(col + term + text);
                    }
                    else if (col.equals("ip")) {
                        cond1.add(col + term + IPUtil.string2ip(text));
                    }
                    else {
                        cond1.add(col + term + "'" + text + "'");
                    }
                }
            }

        }
        catch (Exception e) {
            log.error(e);
            return null;
        }
        return cond1;
    }

    /**
     * 
     * @Title: getUsercode
     * @param domain
     *            String
     * @return int
     * @throws
     * @Description 根据域名计算usercode
     */
    private int getUsercode(String domain) {
        Map<String, String> map = readDomainUserid(confPath + "domain_userid.txt");
        if (map.containsKey(domain)) {
            return computeUsercode(map.get(domain));
        }
        return -1;
    }

    /**
     * 
     * @Title: computeUsercode
     * @param userid
     *            String
     * @return int
     * @throws
     * @Description: 根据userid，计算usercode
     */
    private int computeUsercode(String userid) {
        return (int) Math.round(Math.ceil(Integer.valueOf(userid) / (double) 200));
    }

    /**
     * 
     * @Title: readDomainUserid
     * @param path
     *            String
     * @return Map<String, String>
     * @throws
     * @Description: 读取域名和userid的配置文件
     */
    private Map<String, String> readDomainUserid(String path) {
        return readFile(path);
    }

    /**
     * 
     * @Title: ifCityIspUseridPopid
     * @throws
     * @Description: 判断查询字段里是否包含城市，运营商，用户id和机房id
     */
    private void ifCityIspUseridPopid() {

        if (this.clist != null) {
            for (String str : this.clist) {
                if (str.contains("userid")) {
                    user = readFile(confPath + "cdn_user.txt");
                }
                if (str.contains("isp")) {
                    if (isp == null) {
                        isp = readFile(confPath + "cdn_server_isp.txt");
                    }
                }
                if (str.contains("prv")) {
                    if (city == null) {
                        city = readFile(confPath + "cdn_ip_area.txt");
                    }
                }
                if (str.contains("popid")) {
                    if (popName == null) {
                        popName = readFile(confPath + "cdn_pop.txt");
                    }
                }
            }
        }
    }

    /***
     * 
     * @Title: readFile
     * @param path
     *            String
     * @return Map<String, String>
     * @throws
     * @Description: 读取文件中的数据，生成map
     */
    private Map<String, String> readFile(String path) {

        Map<String, String> map = new HashMap<String, String>();
        File file = new File(path);
        FileInputStream fInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader buffreader = null;
        if (file.exists()) {
            try {
                fInputStream = new FileInputStream(file);
                inputStreamReader = new InputStreamReader(fInputStream, "UTF-8");
                buffreader = new BufferedReader(inputStreamReader);
                String line = null;
                while ((line = buffreader.readLine()) != null) {
                    String[] temp = StringUtils.split(line, ",");
                    map.put(temp[0], temp[1].trim());
                }
            }
            catch (Exception e) {
                log.error(e);
            }
            finally {
                try {
                    if (buffreader != null) {
                        buffreader.close();
                    }
                    if (inputStreamReader != null) {
                        inputStreamReader.close();
                    }
                    if (fInputStream != null) {
                        fInputStream.close();
                    }
                }
                catch (IOException e) {
                    log.error(e);
                }
            }
        }
        else {
            return null;
        }
        return map;
    }

    /**
     * 
     * @Title: ifcache
     * @param sqlSentence
     * @return
     * @throws
     * @Description: 根据查询参数，判断结果是否有缓存
     */
    private boolean ifcache(String sqlSentence) {

        if (resultCache.size() == 0) {
            Map<String, String> temp = readFile(cachePath + "index");
            if (temp == null || temp.size() == 0) {
                return false;
            }
            resultCache.putAll(temp);
        }

        if (resultCache.containsKey(sqlSentence)) {
            File file = new File(cachePath + resultCache.get(sqlSentence) + ".cache");
            if (file.exists()) {
                return true;
            }
            else {
                resultCache.remove(sqlSentence);
                return false;
            }
        }
        return false;
    }

    /**
     * 
     * @Title: readCache
     * @param sqlSentence
     * @throws
     * @Description: 根据参数，读取缓存结果
     */
    private void readCache(String sqlSentence) {
        log.info("read cache ...................");

        File file = new File(cachePath + resultCache.get(sqlSentence) + ".cache");
        FileInputStream fInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader buffreader = null;
        if (file.exists()) {
            try {
                fInputStream = new FileInputStream(file);
                inputStreamReader = new InputStreamReader(fInputStream, "UTF-8");
                buffreader = new BufferedReader(inputStreamReader);
                String line = null;
                line = buffreader.readLine();
                String[] metadata = null;
                if (line != null) {
                    metadata = line.split(",");
                }

                while ((line = buffreader.readLine()) != null) {
                    this.total++;
                    String[] temp = StringUtils.split(line, ",");
                    JSONObject mapOfColValues = new JSONObject();
                    for (int i = 0; i < temp.length; i++) {
                        mapOfColValues.put(metadata[i], temp[i]);
                    }
                    result.add(mapOfColValues);
                }
            }
            catch (Exception e) {
                setMessage(e.getMessage());
                log.error(e);
                this.reslt = ActionSupport.ERROR;
            }
            finally {
                try {
                    if (buffreader != null) {
                        buffreader.close();
                    }
                    if (inputStreamReader != null) {
                        inputStreamReader.close();
                    }
                    if (fInputStream != null) {
                        fInputStream.close();
                    }
                }
                catch (IOException e) {
                    log.error(e);
                }
            }
        }
        this.reslt = ActionSupport.SUCCESS;
    }

    /**
     * 
     * @Title: saveResultCacheToFile
     * @param path
     * @param fileName
     * @throws
     * @Description: 把缓存变量的值写入到index文件中
     */
    private void saveResultCacheToFile(String path, String fileName) {
        File file = new File(path + fileName);

        FileOutputStream fos = null;
        OutputStreamWriter filewriter = null;
        try {
            file.getParentFile().mkdir();
            file.createNewFile();
            fos = new FileOutputStream(file);
            filewriter = new OutputStreamWriter(fos, "UTF-8");
            for (Entry<String, String> entry : resultCache.entrySet()) {
                filewriter.write(entry.getKey() + "," + entry.getValue() + "\n");
            }
            filewriter.flush();
        }
        catch (FileNotFoundException e) {
            log.error(e);
        }
        catch (IOException e) {
            log.error(e);
        }
        finally {
            try {
                if (filewriter != null) {
                    filewriter.close();
                }
                if (fos != null) {
                    fos.close();
                }
            }
            catch (IOException e) {
                log.error(e);
            }
        }

    }

    /**
     * 
     * @Title: sortedSQL
     * @return
     * @throws
     * @Description: 对url中的参数进行排序
     */
    private String sortedSQL() {
        List<String> list = new ArrayList<String>();
        if (this.type != null) {
            for (String str : this.type) {
                list.add(str);
            }
        }

        if (this.clist != null) {
            for (String str : this.clist) {
                list.add(str);
            }
        }

        if (this.startTime != 0) {
            list.add(String.valueOf(this.startTime));
        }
        if (this.endTime != 0) {
            list.add(String.valueOf(this.endTime));
        }

        if (this.limit != 0) {
            list.add(String.valueOf(this.limit));
        }

        if (this.order1 != null) {
            list.add(order1);
        }
        if (this.order2 != null) {
            list.add(order2);
        }
        List<String> temp = this.produceConditionList();

        if (temp != null) {
            list.addAll(temp);
        }
        if (this.interval != 0) {
            list.add(String.valueOf(this.interval));
        }
        Collections.sort(list);
        return list.toString().replace(",", ".");

    }

    private Properties readConf(String path) throws IOException {
        File file = new File(path);
        if (file.exists()) {
            Properties prop = new Properties();
            InputStream in = new BufferedInputStream(new FileInputStream(file));
            prop.load(in);
            in.close();
            return prop;
        }
        return null;
    }

    public static String getTimeStr(int timeInterval) {
        // String time =
        // "from_unixtime(unix_timestamp(timestmp,'yyyyMMddHHmmss')-unix_timestamp(timestmp,'yyyyMMddHHmmss')%"
        // + timeInterval + " + " + timeInterval + ") as time";
        String time = "from_unixtime(unix_timestamp(timestmp,'yyyyMMddHHmmss')-unix_timestamp(timestmp,'yyyyMMddHHmmss')%"
                + timeInterval + ") as time";
        return time;
    }

    public void setType(String[] type) {
        this.type = type;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public JSONArray getResult() {
        return result;
    }

    public void setResult(JSONArray result) {
        this.result = result;
    }

    public long getTotal() {
        return total;
    }

    public void setTotal(long total) {
        this.total = total;
    }

    public void setClist(String[] clist) {
        this.clist = clist;
    }

    public void setOrder1(String order1) {
        this.order1 = order1;
    }

    public void setOrder2(String order2) {
        this.order2 = order2;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public static void main(String[] args) {
        // System.out.println(new
        // CdnlogErrorStatusQueryTimeIntervalAction().computeUsercode("2896"));
        System.out.println(getTimeStr(600));
    }

}
