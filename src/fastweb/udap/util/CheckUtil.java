/** 
 * @Title: Check.java 
 * @Package com.fastweb.cdnlog.duowan.check 
 * @author LiFuqiang 
 * @date 2016年8月11日 上午10:21:10 
 * @version V1.0 
 * @Description: TODO(用一句话描述该文件做什么) 
 * Update Logs: 
 * **************************************************** * Name: * Date: * Description: ****************************************************** 
 */
package fastweb.udap.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jettison.json.JSONObject;

/**
 * @ClassName: Check
 * @author LiFuqiang
 * @date 2016年8月11日 上午10:21:10
 * @Description: 三方数据对比实现，heka,fwlog,计数器
 */
public class CheckUtil {
    private List<CsNode> data = new ArrayList<CsNode>();
    private String domain;
    private String day; // 2016/07/01
    private String outputPath;
    private float log95 = 0.0f;
    private long timeLog95 = 0l;
    private long timeCnt95 = 0l;
    private float cnt95 = 0.0f;
    private float heka95 = 0.0f;
    private long timeHeka95 = 0l;

    private float logMax = 0.0f;
    private float cntMax = 0.0f;
    private float hekaMax = 0.0f;
    private long timeLogMax = 0l;
    private long timeCntMax = 0l;
    private long timeHekaMax = 0l;

    public CheckUtil(String domain, String day, String outputPath) throws Exception {
        this.domain = domain;
        this.day = day;
        this.outputPath = outputPath;
        initial();
    }

    public void initial() throws Exception {
        Map<Long, Long> cnt = GetDomainCsFromCounter.getDomainFiveMinuteCs(this.domain,
                this.day.replace("/", "-"));
        Map<Long, Long> log = new TreeMap<Long, Long>(getDataFromFwlog(this.domain, this.day));
        Map<Long, Long> heka = getDataFromHeka(domain, day);
        if (!this.day.equals(DateUtil.getToday())) {
            cnt.putAll(GetDomainCsFromCounter.getDomainFiveMinuteCs(this.domain,
                    DateUtil.nextDay(day).replace("/", "-")));
            log.putAll(getDataFromFwlog(this.domain, DateUtil.nextDay(this.day)));
            heka.putAll(getDataFromHeka(domain, DateUtil.nextDay(day)));
        }

        for (Entry<Long, Long> entry : log.entrySet()) {
            long time = entry.getKey();
            long logCs = entry.getValue();
            long cntCs = 0l;
            long hekaCs = 0l;
            if (cnt.containsKey(time)) {
                cntCs = cnt.get(time);
            }
            if (heka.containsKey(time)) {
                hekaCs = heka.get(time);
            }
            CsNode cnode = new CsNode(time, logCs, cntCs, hekaCs);
            if (time >= (DateUtil.getStartTime(this.day) + 300)
                    && time <= DateUtil.getEndTime(this.day) + 300) {
                data.add(cnode);
            }
        }
    }

    public void outputResult() {
        FileUtil.listToFile(outputPath, getResult(), "gbk");
    }

    public List<String> getResult() {
        compute();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        DecimalFormat decimalFormat = new DecimalFormat(".00");// 构造方法的字符格式这里如果小数不足2位,会以0补足.
        Collections.sort(data, new CsNodeSortByDate());
        List<String> result = new ArrayList<String>();
        result.add("time," + "Fwlog-5minutes-band(Mbps)," + "Heka-5minutes-band(Mbps),"
                + "Counter-5minutes-band(Mbps)," + "(Counter-Heka)/counter,"
                + "(Counter-Fwlog)/counter");
        for (CsNode c : this.data) {
            String date = sdf.format(new Date(c.getTime() * 1000));
            result.add(date + "," + c.getLogBand() + "," + c.getHekaBand() + "," + c.getCntBand()
                    + "," + decimalFormat.format(c.getHekaCntErrorPercent()) + "%,"
                    + decimalFormat.format(c.getErrorPercent()) + "%");
            // + decimalFormat.format(c.getHekaFwlogErrorPercent()) + "%");
        }
        result.add("\n");
        result.add("Fwlog95," + sdf.format(new Date(this.timeLog95 * 1000)) + ","
                + decimalFormat.format(this.log95));
        result.add("Heka95," + sdf.format(new Date(this.timeHeka95 * 1000)) + ","
                + decimalFormat.format(this.heka95));
        result.add("Counter95," + sdf.format(new Date(this.timeCnt95 * 1000)) + ","
                + decimalFormat.format(this.cnt95));
        float error95 = this.log95 - this.cnt95;
        float hekaError95 = this.heka95 - this.cnt95;
        String percent95 = decimalFormat.format(error95 / this.cnt95 * 100);
        String hekaPercent95 = decimalFormat.format(hekaError95 / this.cnt95 * 100);
        result.add("(Counter95-Heka95)/counter95,-" + decimalFormat.format(hekaError95) + ",-"
                + hekaPercent95 + "%");
        result.add("(Counter95-Fwlog95)/counter95,-" + decimalFormat.format(error95) + ",-"
                + percent95 + "%");
        result.add("\n");

        float errorMax = this.logMax - this.cntMax;
        String percentMax = decimalFormat.format(errorMax / this.cntMax * 100);
        float hekaErrorMax = this.hekaMax - this.cntMax;
        String hekaPercentMax = decimalFormat.format(hekaErrorMax / this.cntMax * 100);
        result.add("FwlogMax," + sdf.format(new Date(this.timeLogMax * 1000)) + ","
                + decimalFormat.format(this.logMax));
        result.add("HekaMax," + sdf.format(new Date(this.timeHekaMax * 1000)) + ","
                + decimalFormat.format(this.hekaMax));
        result.add("CounterMax," + sdf.format(new Date(this.timeCntMax * 1000)) + ","
                + decimalFormat.format(this.cntMax));
        result.add("(CounterMax-HekaMax)/hekaMax,-" + decimalFormat.format(hekaErrorMax) + ",-"
                + hekaPercentMax + "%");
        result.add("(CounterMax-FwlogMax)/hekaMax,-" + decimalFormat.format(errorMax) + ",-"
                + percentMax + "%");
        result.add("\n");

        // result.add("偏差: 计数器-日志");
        // result.add("偏差百分比: 偏差/计数器");
        return result;

    }

    public void compute() {
        int pos95 = (int) (data.size() * 0.95);

        Collections.sort(data, new CsNodeSortByLogCs());

        this.logMax = data.get(data.size() - 1).getLogBand();
        this.timeLogMax = data.get(data.size() - 1).getTime();

        this.log95 = this.data.get(pos95).getLogBand();
        this.timeLog95 = this.data.get(pos95).getTime();

        Collections.sort(data, new CsNodeSortByCntCs());

        this.cntMax = data.get(data.size() - 1).getCntBand();
        this.timeCntMax = data.get(data.size() - 1).getTime();

        this.cnt95 = data.get(pos95).getCntBand();
        this.timeCnt95 = data.get(pos95).getTime();

        Collections.sort(data, new CsNodeSortByHekaCs());
        this.hekaMax = this.data.get(data.size() - 1).getHekaBand();
        this.timeHekaMax = this.data.get(data.size() - 1).getTime();
        this.heka95 = this.data.get(pos95).getHekaBand();
        this.timeHeka95 = this.data.get(pos95).getTime();

    }

    /**
     * 
     * @Title: getDataFromEs
     * @param domain
     * @param day
     * @return
     * @throws Exception
     * @throws
     * @Description: 时间格式：2016/07/01，时间精确到某一天
     */
    public static Map<Long, Long> getDataFromEs(String domain, String day) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        long startTime = sdf.parse(day).getTime() / 1000;
        long endTime = startTime + 86399;
        Map<Long, Long> map = new HashMap<Long, Long>();
        HttpClient httpclient = new DefaultHttpClient();
        String url = "http://115.231.46.83:8088/udap/fiveminutecscountduowan.action?domain="
                + domain + "&begin=" + startTime + "&end=" + endTime;
        HttpGet httpgets = new HttpGet(url);
        HttpResponse response = httpclient.execute(httpgets);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            InputStream instreams = entity.getContent();
            map.putAll(convertStreamToMap(instreams));
            httpgets.abort();
            instreams.close();
        }
        return map;
    }

    /**
     * 
     * @Title: getDataFromEs
     * @param domain
     * @param day
     * @return
     * @throws Exception
     * @throws
     * @Description: 时间格式：2016/07/01，时间精确到某一天
     */
    public static Map<Long, Long> getDataFromHeka(String domain, String day) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        long startTime = sdf.parse(day).getTime() / 1000;
        long endTime = startTime + 86399;
        Map<Long, Long> map = new HashMap<Long, Long>();
        HttpClient httpclient = new DefaultHttpClient();
        String url = "http://udap.fwlog.cachecn.net:8088/udap/fiveminutecscountduowancompare.action?domain="
                + domain + "&begin=" + startTime + "&end=" + endTime;
        HttpGet httpgets = new HttpGet(url);
        HttpResponse response = httpclient.execute(httpgets);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            InputStream instreams = entity.getContent();
            map.putAll(convertStreamToMap(instreams));
            httpgets.abort();
            instreams.close();
        }
        return map;
    }

    /**
     * 
     * @Title: getDataFromEs
     * @param domain
     * @param day
     * @return
     * @throws Exception
     * @throws
     * @Description: 时间格式：2016/07/01，时间精确到某一天
     */
    public static Map<Long, Long> getDataFromFwlog(String domain, String day) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        long startTime = sdf.parse(day).getTime() / 1000;
        long endTime = startTime + 86399;
        Map<Long, Long> map = new HashMap<Long, Long>();
        HttpClient httpclient = new DefaultHttpClient();
        String url = "http://udap.fwlog.cachecn.net:8088/udap/fiveminutecscountduowan.action?domain="
                + domain + "&begin=" + startTime + "&end=" + endTime;
        HttpGet httpgets = new HttpGet(url);
        HttpResponse response = httpclient.execute(httpgets);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            InputStream instreams = entity.getContent();
            map.putAll(convertStreamToMap(instreams));
            httpgets.abort();
            instreams.close();
        }
        return map;
    }

    /**
     * 
     * @Title: getDataFromEs
     * @param domain
     * @param day
     * @return
     * @throws Exception
     * @throws
     * @Description: 时间格式：2016/07/01，时间精确到某一天
     */
    public static Map<Long, Long> getDataFromEs2(String domain, String day) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        long startTime = sdf.parse(day).getTime() / 1000;
        long endTime = startTime + 86399;
        Map<Long, Long> map = new HashMap<Long, Long>();
        HttpClient httpclient = new DefaultHttpClient();
        String url = "http://udap.fwlog.cachecn.net:8088/udap/fiveminutedircscount.action?domain="
                + domain + "&begin=" + startTime + "&end=" + endTime;
        HttpGet httpgets = new HttpGet(url);
        HttpResponse response = httpclient.execute(httpgets);
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            InputStream instreams = entity.getContent();
            map.putAll(convertStreamToMap2(instreams));
            httpgets.abort();
            instreams.close();
        }
        return map;
    }

    private static Map<Long, Long> convertStreamToMap(InputStream is) throws Exception {
        Map<Long, Long> map = new HashMap<Long, Long>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        JSONObject json = null;
        try {
            while ((line = reader.readLine()) != null) {
                // if (line.startsWith("14")) {
                // String[] temp = line.split(" ");
                // long date = Long.valueOf(temp[0].trim()) - 300;
                // map.put(date, Long.valueOf(temp[1].trim()));
                // }
                json = new JSONObject(line);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        JSONObject result = json.getJSONArray("result").getJSONObject(0).getJSONObject("timestamp");
        Iterator<String> iterator = result.keys();
        while (iterator.hasNext()) {
            String key = iterator.next();
            long time = Long.valueOf(key);
            map.put(time, result.getLong(key));
        }
        return map;
    }

    public static Map<Long, Long> getDataFromLog(String domain, String day) throws Exception {
        Map<Long, Long> log = getDataFromHdfs(domain, day);
        Map<Long, Long> logNextDay = null;
        if (!day.equals(DateUtil.getToday())) {
            logNextDay = getDataFromHdfs(domain, DateUtil.nextDay(day));
        }
        for (Entry<Long, Long> entry : logNextDay.entrySet()) {
            if (log.containsKey(entry.getKey())) {
                log.put(entry.getKey(), log.get(entry.getKey()) + entry.getValue());
            }
        }
        return log;
    }

    public static Map<Long, Long> getDataFromHdfs(String domain, String day) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        long startTime = sdf.parse(day).getTime() / 1000;
        long endTime = startTime + 86399;
        Map<Long, Long> map = new HashMap<Long, Long>();
        HttpClient httpclient = new DefaultHttpClient();
        String url = "http://master202:50070/webhdfs/v1/user/check_band/daily/duowan/norepair/"
                + day + "/" + domain + "/part-r-00000" + "?op=OPEN";
        URLConnection conn = new URL(url).openConnection();
        InputStream instreams = conn.getInputStream();
        map.putAll(convertStreamToMapHdfs(instreams));

        instreams.close();

        return map;
    }

    private static Map<Long, Long> convertStreamToMapHdfs(InputStream is) throws Exception {
        Map<Long, Long> map = new HashMap<Long, Long>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        JSONObject json = null;
        try {
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("14")) {
                    String[] temp = line.split(",");
                    long date = Long.valueOf(temp[0].trim());
                    map.put(date, Long.valueOf(temp[1].trim()));
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    private static Map<Long, Long> convertStreamToMap2(InputStream is) throws Exception {
        Map<Long, Long> map = new HashMap<Long, Long>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        String line = null;
        JSONObject json = null;
        try {
            while ((line = reader.readLine()) != null) {
                // if (line.startsWith("14")) {
                // String[] temp = line.split(" ");
                // long date = Long.valueOf(temp[0].trim()) - 300;
                // map.put(date, Long.valueOf(temp[1].trim()));
                // }
                json = new JSONObject(line);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        JSONObject result = json.getJSONArray("result").getJSONObject(0).getJSONObject("domain");
        String domain = (String) (result.keys().next());
        JSONObject result2 = result.getJSONObject(domain);
        Iterator<String> iter = result2.keys();
        while (iter.hasNext()) {
            String time = iter.next();
            long timeL = Long.valueOf(time);

            JSONObject result3 = result2.getJSONObject(time);
            Iterator<String> iterD = result3.keys();
            long sum = 0l;
            while (iterD.hasNext()) {
                String dir = iterD.next();
                JSONObject result4 = result3.getJSONObject(dir);
                Iterator<String> iterF = result4.keys();
                while (iterF.hasNext()) {
                    long flowSize = result4.getLong(iterF.next());
                    sum = sum + flowSize;
                    System.out.println(flowSize);
                }
            }
            map.put(timeL, sum);
        }
        return map;
    }

    public static Map<Long, Long> getDataFromFile(String domain, String day) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        String path = "F:\\业务资料\\华多\\temp\\" + domain;
        InputStream instreams = new FileInputStream(new File(path));
        Map<Long, Long> map = new HashMap<Long, Long>();
        map.putAll(convertStreamToMapHdfs(instreams));

        instreams.close();
        return map;
    }

    public static class CsNode {
        private long time;
        private long logCs;
        private long cntCs;
        private long hekaCs;

        public CsNode(long time, long logCs, long cntCs, long hekaCs) {
            super();
            this.time = time;
            this.logCs = logCs;
            this.cntCs = cntCs;
            this.hekaCs = hekaCs;
        }

        public float getError() {
            return getCntBand() - getLogBand();
        }

        public float getHekaFwlogError() {
            return getHekaBand() - getLogBand();
        }

        public float getHekaFwlogErrorPercent() {
            return getHekaFwlogError() / getHekaBand() * 100;
        }

        public float getHekaCntError() {
            return getCntBand() - getHekaBand();
        }

        public float getHekaCntErrorPercent() {
            return getHekaCntError() / getCntBand() * 100;
        }

        public float getHekaBand() {
            return (float) this.hekaCs * 8 / 300 / 1024 / 1024;
        }

        public float getErrorPercent() {
            return getError() / getCntBand() * 100;
        }

        public float getLogBand() {
            return (float) this.logCs * 8 / 300 / 1024 / 1024;
        }

        public float getCntBand() {
            return (float) this.cntCs * 8 / 300 / 1024 / 1024;
        }

        public long getTime() {
            return time;
        }

        public long getLogCs() {
            return logCs;
        }

        public long getCntCs() {
            return cntCs;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public void setLogCs(long logCs) {
            this.logCs = logCs;
        }

        public void setCntCs(long cntCs) {
            this.cntCs = cntCs;
        }

        public long getHekaCs() {
            return hekaCs;
        }

        public void setHekaCs(long hekaCs) {
            this.hekaCs = hekaCs;
        }

    }

    public static class CsNodeSortByDate implements Comparator<CsNode> {
        @Override
        public int compare(CsNode o1, CsNode o2) {
            if (o1.getTime() > o2.getTime()) {
                return 1;
            }
            else if (o1.getTime() < o2.getTime()) {
                return -1;
            }
            return 0;
        }
    }

    /**
     * 
     * @ClassName: CsNodeSortByLogCs
     * @author LiFuqiang
     * @date 2016年8月12日 上午9:42:24
     * @Description: 升序排列
     */
    public static class CsNodeSortByLogCs implements Comparator<CsNode> {
        @Override
        public int compare(CsNode o1, CsNode o2) {
            if (o1.getLogCs() > o2.getLogCs()) {
                return 1;
            }
            else if (o1.getLogCs() < o2.getLogCs()) {
                return -1;
            }
            return 0;
        }
    }

    public static class CsNodeSortByCntCs implements Comparator<CsNode> {
        @Override
        public int compare(CsNode o1, CsNode o2) {
            if (o1.getCntCs() > o2.getCntCs()) {
                return 1;
            }
            else if (o1.getCntCs() < o2.getCntCs()) {
                return -1;
            }
            return 0;
        }
    }

    public static class CsNodeSortByHekaCs implements Comparator<CsNode> {
        @Override
        public int compare(CsNode o1, CsNode o2) {
            if (o1.getHekaCs() > o2.getHekaCs()) {
                return 1;
            }
            else if (o1.getHekaCs() < o2.getHekaCs()) {
                return -1;
            }
            return 0;
        }
    }

    /**
     * @Title: main
     * @param args
     * @throws IOException
     * @throws
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */
    public static void main(String[] args) throws Exception {
        main2(args);
        // test();
        // System.out.println(getDataFromHdfs("w5.dwstatic.com", "2016/08/24"));
    }

    public static void test2() {

    }

    public static void test() {

        String path = "F:\\业务资料\\华多\\日志\\25-07";
        List<String> lines = new ArrayList<String>();
        for (File file : new File(path).listFiles()) {
            lines.addAll(FileUtil.readFile(file));
        }
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH);
        Map<Long, Long> log = new TreeMap<Long, Long>();
        for (String line : lines) {
            try {
                String[] temp = line.split("#_#");
                // System.out.println(line);
                long timeT = sdf.parse(temp[3].replace("[", "").replace("]", "")).getTime() / 1000;
                long time = timeT - timeT % 300L + 300L;
                long size = Long.valueOf(temp[14]);
                if (log.containsKey(time)) {
                    log.put(time, log.get(time) + size);
                }
                else {
                    log.put(time, size);
                }
            }
            catch (Exception e) {
                System.out.println(line);
            }
        }
        List<String> result = new ArrayList<String>();
        SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd HH:mm");
        for (Entry<Long, Long> entry : log.entrySet()) {
            String time = sdf2.format(new Date(entry.getKey() * 1000));
            float logBand = (float) entry.getValue() * 8 / 300 / 1024 / 1024;
            result.add(time + "," + logBand);
        }
        FileUtil.listToFile("F:\\业务资料\\华多\\日志\\test-25-07.csv", result, "gbk");
    }

    public static void main2(String[] args) throws Exception {
        String[] domains = { "w5.dwstatic.com", "mw5.dwstatic.com", "aipai.w5.dwstatic.com",
                "funbox.w5.dwstatic.com", "dl.vip.yy.com", "bw5.dwstatic.com" };
        String[] domains1 = { "wasu.cloudcdn.net" };
        String[] domains2 = { "mw5.dwstatic.com", "bw5.dwstatic.com" };
        String[] domains3 = { "dd.myapp.com", "dlied1.fastweb.cdn.qq.com",
                "update2.fastweb.cdn.qq.com", "qqpic.fastweb.qq.com" };
        String[] domains4 = { "s6.pstatp.com" };
        String[] domains5 = { "img03.imgcdc.com", "img01.imgcdc.com", "img02.imgcdc.com",
                "img04.imgcdc.com" };
        String[] domains6 = { "dl.vip.yy.com" };
        String day = "2016/09/09";
        String outPath = "F:\\业务资料\\日志对账业务\\华多\\华多-heka-fwlog-计数器比对-" + day.replace("/", "-")
                + "\\";
        for (String domain : domains6) {
            CheckUtil check = new CheckUtil(domain, day, outPath + domain + "-"
                    + day.replace("/", "-") + "-" + System.currentTimeMillis() + ".csv");
            check.outputResult();
        }
    }
}
