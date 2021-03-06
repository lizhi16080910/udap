/**
 * lifq:2017/01/17
 */
package fastweb.udap.web.action;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.ServletActionContext;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * 后缀流量
 */
@Namespace("/")
@Action(value = "cdnlogslowratepercentaction", results = {
        @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties",
                "message" }),
        @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties",
                "message" }) })
public class CdnlogSlowRatePercentAction extends ActionSupport {
    private final Log LOG = LogFactory.getLog(getClass());
    private static final long serialVersionUID = 1L;

    static private String indexPrefix = "cdnlog.boss.flowsize";

    public static final String INDEX = "cdnlog.rate.slow.percent";
    public static final String DOMAIN = "domain";
    public static final String HOST = "host";
    public static final String PROVINCE = "province";
    public static final String ISP = "isp";
    public static final String COUNT = "count";
    public static final String IF_SLOW = "ifSlow";
    public static final String TIME = "time";

    /* 错误信息 */
    private String message;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
    private SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
    private String date = null;

    /* */
    private String domain = null;
    private String host = null;
    private JSONObject result = null;

    @Override
    public String execute() throws Exception {
        // this.domain = "kwmov.a.yximgs.com";
        // this.host = "ctl-js-222-186-142-016";

        LOG.info("domain is " + this.domain);
        LOG.info("host is " + this.host);
        LOG.info("date is " + this.date);
        long date = sdf2.parse(this.date).getTime() / 1000l;

        Map<Key, List<Value>> mmp = new HashMap<Key, List<Value>>();
        try {
            Client client = EsClientFactory.createClient();
            SearchRequestBuilder searchRequestBuilder = null;
            try {

                BoolQueryBuilder allQuery = QueryBuilders.boolQuery();

                // 条件
                if (this.domain != null) {
                    allQuery = allQuery.must(QueryBuilders.termQuery(DOMAIN, this.domain));
                }
                if (this.host != null) {
                    allQuery = allQuery.must(QueryBuilders.termQuery(HOST, this.host));
                }
                if (this.date != null) {
                    allQuery = allQuery.must(QueryBuilders.termQuery(TIME, date));
                }

                BoolQueryBuilder statusQuery = QueryBuilders.boolQuery();
                TermQueryBuilder correctQuery = QueryBuilders.termQuery(IF_SLOW, "1");
                TermQueryBuilder errorQuery = QueryBuilders.termQuery(IF_SLOW, "0");
                BoolQueryBuilder correctQueryBuilder = QueryBuilders.boolQuery().must(correctQuery);
                BoolQueryBuilder errorQueryBuilder = QueryBuilders.boolQuery().must(errorQuery);
                statusQuery.should(correctQueryBuilder);
                statusQuery.should(errorQueryBuilder);
                allQuery.must(statusQuery);

                AbstractAggregationBuilder cSum = AggregationBuilders.sum(COUNT.toUpperCase())
                        .field(COUNT);
                AbstractAggregationBuilder statusKey = AggregationBuilders
                        .terms(IF_SLOW.toUpperCase()).field(IF_SLOW).subAggregation(cSum)
                        .size(Integer.MAX_VALUE);
                AbstractAggregationBuilder hostKey = AggregationBuilders.terms(HOST.toUpperCase())
                        .field(HOST).subAggregation(statusKey).size(Integer.MAX_VALUE);
                AbstractAggregationBuilder timeKey = AggregationBuilders.terms(TIME.toUpperCase())
                        .field(TIME).subAggregation(hostKey).size(Integer.MAX_VALUE);
                AbstractAggregationBuilder provinceKey = AggregationBuilders
                        .terms(PROVINCE.toUpperCase()).field(PROVINCE).subAggregation(timeKey)
                        .size(Integer.MAX_VALUE);
                AbstractAggregationBuilder ispKey = AggregationBuilders.terms(ISP.toUpperCase())
                        .field(ISP).subAggregation(provinceKey).size(Integer.MAX_VALUE);
                AbstractAggregationBuilder domainKey = AggregationBuilders
                        .terms(DOMAIN.toUpperCase()).field(DOMAIN).subAggregation(ispKey)
                        .size(Integer.MAX_VALUE);

                searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0)
                        .setSize(0).addAggregation(domainKey);

                LOG.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": "
                        + searchRequestBuilder.toString());
                SearchResponse response = searchRequestBuilder.execute().actionGet();

                // System.out.println(response);
                LOG.info(response);
                StringTerms domainAgge = response.getAggregations().get(DOMAIN.toUpperCase());

                for (Bucket bucket : domainAgge.getBuckets()) {
                    String domain = bucket.getKey(); // domain

                    StringTerms ispAgge = bucket.getAggregations().get(ISP.toUpperCase());
                    for (Bucket ispBucket : ispAgge.getBuckets()) {
                        String isp = ispBucket.getKey(); // isp
                        StringTerms prvAgge = ispBucket.getAggregations().get(
                                PROVINCE.toUpperCase());
                        for (Bucket timeBucket : prvAgge.getBuckets()) {
                            String province = timeBucket.getKey(); // province
                            LongTerms timeAgge = timeBucket.getAggregations().get(
                                    TIME.toUpperCase());
                            for (Bucket hostBucket : timeAgge.getBuckets()) {
                                String time = hostBucket.getKey(); // time
                                StringTerms hostAgge = hostBucket.getAggregations().get(
                                        HOST.toUpperCase());
                                for (Bucket statusBucket : hostAgge.getBuckets()) {
                                    String host = statusBucket.getKey(); // host
                                    LongTerms statusAgge = statusBucket.getAggregations().get(
                                            IF_SLOW.toUpperCase());
                                    for (Bucket sta : statusAgge.getBuckets()) {
                                        String ifSlow = sta.getKey(); // ifSlow
                                        Sum count = sta.getAggregations().get(COUNT.toUpperCase());
                                        long value = ((Double) count.getValue()).longValue();
                                        Key kkey = new Key(Long.valueOf(time), province, host, isp,
                                                domain);
                                        Value val = new Value(Integer.valueOf(ifSlow), value);
                                        List<Value> list = mmp.get(kkey);
                                        if (list == null) {
                                            list = new ArrayList<Value>();
                                        }
                                        list.add(val);
                                        mmp.put(kkey, list);
                                    }
                                }
                            }
                        }
                    }
                }
                // System.out.println(mmp);
            }
            finally {
                client.close();
            }
            JSONObject mapOfColValues = new JSONObject();
            JSONArray jsonArray = new JSONArray();
            long total = 0l;
            long slowTotal = 0l;
            mapOfColValues.put(DOMAIN, this.domain);
            mapOfColValues.put(HOST, this.host);
            mapOfColValues.put("date_time", sdf.format(new Date(date * 1000)));
            List<JSONObject> listJson = new ArrayList<JSONObject>();
            JSONObject OtherJsonObj = new JSONObject();
            for (Entry<Key, List<Value>> entry : mmp.entrySet()) {
                String province = entry.getKey().getProvince();
                String isp = entry.getKey().getIsp();
                long normalCount = 0l;
                long slowCount = 0l;
                for (Value val : entry.getValue()) {
                    if (val.getIfSlow() == 1)
                        slowCount = val.getCount();
                    if (val.getIfSlow() == 0)
                        normalCount = val.getCount();
                }
                long totalCount = normalCount + slowCount;
                total = total + totalCount;
                slowTotal = slowTotal + slowCount;
                // jsonArray.add(jsonObj);
                if (isp.endsWith("other")) {
                    OtherJsonObj.put(PROVINCE, province);
                    OtherJsonObj.put(ISP, isp);
                    OtherJsonObj.put("total_click", totalCount);
                    OtherJsonObj.put("slow_click", slowCount);
                    continue;
                }
                JSONObject jsonObj = new JSONObject();
                jsonObj.put(PROVINCE, province);
                jsonObj.put(ISP, isp);
                jsonObj.put("total_click", totalCount);
                jsonObj.put("slow_click", slowCount);
                listJson.add(jsonObj);
            }
            Collections.sort(listJson, new JsonSortByCount());
            for (JSONObject jsonObj : listJson) {
                jsonArray.add(jsonObj);
            }
            jsonArray.add(OtherJsonObj);
            mapOfColValues.put("total_click", total);
            mapOfColValues.put("slow_click", slowTotal);
            mapOfColValues.put("list", jsonArray);
            result = mapOfColValues;
            // System.out.println(mapOfColValues);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {

        }
        return ActionSupport.SUCCESS;
    }

    public JSONObject getResult() {
        return result;
    }

    public String getMessage() {
        return message;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setHost(String host) {
        this.host = host;
    }

    /* check the validity of parameters pass in */
    /* 参数校验 */
    @Override
    public void validate() {
        super.validate();

        // if (begin > end) {
        // setMessage("开始时间戳大于结束时间戳");
        // return;
        // }
        // else {
        // if (metric.endsWith("one") && end - begin > 3 * 60 * 1000) {
        // setMessage("分钟数据支持三小时查询");
        // return;
        // }
        // else if (end - begin > 45 * 24 * 60 * 1000) {
        // setMessage("5分钟和小时数据支持45天查询");
        // return;
        // }
        // }

    }

    /* 根据用户metric参数选择需要查询的Es index */
    /* 所有支持的metric，会在valid函数中对metric参数做检验，不在该列表中会报错 */
    public static class Key {
        private long time = 0l;
        private String province = null;
        private String host = null;
        private String isp = null;
        private String domain = null;

        public Key(long time, String province, String host, String isp, String domain) {
            super();
            this.time = time;
            this.province = province;
            this.host = host;
            this.isp = isp;
            this.domain = domain;
        }

        public long getTime() {
            return time;
        }

        public String getProvince() {
            return province;
        }

        public String getHost() {
            return host;
        }

        public String getIsp() {
            return isp;
        }

        public String getDomain() {
            return domain;
        }

        public void setTime(long time) {
            this.time = time;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public void setIsp(String isp) {
            this.isp = isp;
        }

        public void setDomain(String domain) {
            this.domain = domain;
        }

        @Override
        public String toString() {
            return "Key [time=" + time + ", province=" + province + ", host=" + host + ", isp="
                    + isp + ", domain=" + domain + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((domain == null) ? 0 : domain.hashCode());
            result = prime * result + ((host == null) ? 0 : host.hashCode());
            result = prime * result + ((isp == null) ? 0 : isp.hashCode());
            result = prime * result + ((province == null) ? 0 : province.hashCode());
            result = prime * result + (int) (time ^ (time >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Key other = (Key) obj;
            if (domain == null) {
                if (other.domain != null)
                    return false;
            }
            else if (!domain.equals(other.domain))
                return false;
            if (host == null) {
                if (other.host != null)
                    return false;
            }
            else if (!host.equals(other.host))
                return false;
            if (isp == null) {
                if (other.isp != null)
                    return false;
            }
            else if (!isp.equals(other.isp))
                return false;
            if (province == null) {
                if (other.province != null)
                    return false;
            }
            else if (!province.equals(other.province))
                return false;
            if (time != other.time)
                return false;
            return true;
        }
    }

    public static class Value {
        private int ifSlow = -1;
        private long count = -1l;

        // private Map<Integer, Long> map = new HashMap<Integer, Long>();

        public Value(int ifSlow, long count) {
            super();
            this.ifSlow = ifSlow;
            this.count = count;
        }

        public int getIfSlow() {
            return ifSlow;
        }

        public long getCount() {
            return count;
        }

        public void setIfSlow(int ifSlow) {
            this.ifSlow = ifSlow;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "Value [ifSlow=" + ifSlow + ", count=" + count + "]";
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (count ^ (count >>> 32));
            result = prime * result + ifSlow;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Value other = (Value) obj;
            if (count != other.count)
                return false;
            if (ifSlow != other.ifSlow)
                return false;
            return true;
        }
    }

    public static class JsonSortByCount implements Comparator<JSONObject> {

        @Override
        public int compare(JSONObject o1, JSONObject o2) {
            if (Long.valueOf(o1.get("total_click").toString()) > Long.valueOf(o2.get("total_click")
                    .toString())) {
                return -1;
            }
            else if (Long.valueOf(o1.get("total_click").toString()) < Long.valueOf(o2.get(
                    "total_click").toString())) {
                return 1;
            }
            return 0;
        }
    }

    public static void main(String[] aegs) throws Exception {
        new CdnlogSlowRatePercentAction().execute();
    }

}
