/** 
 * @Title: GetDomainCs.java 
 * @Package com.fastweb.cdnlog.bandcheck.util 
 * @author LiFuqiang 
 * @date 2016年8月5日 上午11:38:59 
 * @version V1.0 
 * @Description: TODO(用一句话描述该文件做什么) 
 * Update Logs: 
 * **************************************************** * Name: * Date: * Description: ****************************************************** 
 */
package fastweb.udap.util;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
 * @ClassName: GetDomainCs
 * @author LiFuqiang
 * @date 2016年8月5日 上午11:38:59
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class GetDomainCsFromCounter {
    private static final Log LOG = LogFactory.getLog(GetDomainCsFromCounter.class);
    public static String url = "https://cdnboss-api.fastweb.com.cn/report/get_bandwidth_traffic";

    // public static String url1 =
    // "https://cdnboss-api.fastweb.com.cn/Base/Domain/get_domain_list_group";

    /**
     * @Title: main
     * @param args
     * @throws JSONException
     * @throws ParseException
     * @throws
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */
    public static void main(String[] args) throws Exception {
        Map resultmap = getDomainFiveMinuteCs("tpr-wow.client03.pdl.wow.battlenet.com.cn",
                "2016-08-01");
        Object[] key = resultmap.keySet().toArray();
        Arrays.sort(key);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < key.length; i++) {
            sb.append(key[i] + " " + resultmap.get(key[i]) + "/n");
        }
    }

    /**
     * 
     * @Title: getDomainFiveMinuteCs
     * @param domain
     * @param date
     * @return
     * @throws Exception
     * @throws
     * @Description: 时间参数格式：2016-07-01，时间精确到某一天，返回map类型的数据,按天读取计数器的值
     */
    public static Map<Long, Long> getDomainFiveMinuteCs(String domain, String day) throws Exception {
        Map<Long, Long> map = new HashMap<Long, Long>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");

        String fcname = "cdnwhbigdata";
        String fckey = "fastweb_whbigdata";
        // 拼接字符串
        StringBuilder sb = new StringBuilder();
        sb.append("{\"fcname\":\"");
        sb.append(fcname);
        String fctoken = DigestUtils.md5Hex(new SimpleDateFormat("yyyyMMdd").format(new Date(System
                .currentTimeMillis())) + DigestUtils.md5Hex(fckey));
        sb.append("\"," + "\"fctoken\":\"");
        sb.append(fctoken);
        sb.append("\"," + "\"domains\":\"");
        sb.append(domain);
        sb.append("\"," + "\"date\":\"");
        sb.append(day);

        sb.append("\"}");
        LOG.info(sb);

        JSONObject result = null;
        JSONArray data_list = null;
        int status = 0;
        result = new JSONObject(UrlUtil.postData(url, sb.toString()));
        status = result.getInt("status");
        String info = result.getString("info");
        String info_utf_8 = new String(info.getBytes(), "UTF-8");
        LOG.info("status: " + status);
        LOG.info("info: " + info_utf_8);

        data_list = new JSONArray(result.getJSONObject("result").getString("data_list"));

        int length = data_list.length();
        for (int i = 0; i < length; i++) {
            JSONObject jsonObj = data_list.getJSONObject(i);
            long time = Long.valueOf(sdf.parse(jsonObj.getString("time").trim()).getTime() / 1000) - 300;
            BigDecimal cs = new BigDecimal(jsonObj.getString("traffic_size").trim());
            map.put(time, cs.longValue());
        }
        return map;
    }
}
