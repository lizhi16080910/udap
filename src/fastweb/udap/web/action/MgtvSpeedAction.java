package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang.xwork.StringUtils;
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
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * 和湖南卫视的维度一样，算法一致，过滤条件一致
 */
@Namespace("/")
@Action(value = "mgtvspeed", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class MgtvSpeedAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private Map<String, Map<String, Map<String, Long>>> result_ = new TreeMap<String, Map<String, Map<String, Long>>>();

	private List<Map<String, String>> result = new ArrayList<Map<String, String>>();

	public static final String INDEX = "cdnlog.mgtv.speed";
	private String message;
	private String prv;
	private String isp;
	private long begin;
	private long end;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		if (initSpeed() == 0) {
			return ActionSupport.ERROR;
		}
		return ActionSupport.SUCCESS;
	}

	// 初始化慢速比数据
	public int initSpeed() {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("fivetime").from(begin).to(end));

			if(StringUtils.isNotBlank(prv)){
				allQuery.must(QueryBuilders.termsQuery("prv", this.prv.split(",")));
			}
			if(StringUtils.isNotBlank(isp)){
				allQuery.must(QueryBuilders.termsQuery("isp", this.isp.split(",")));
			}
			
			BoolQueryBuilder statusQuery = QueryBuilders.boolQuery();
			TermQueryBuilder termQuery = QueryBuilders.termQuery("status", "1");
			TermsQueryBuilder aQuery = QueryBuilders.termsQuery("status", "0");
			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(termQuery);
			BoolQueryBuilder aQueryBuilder = QueryBuilders.boolQuery().must(aQuery);
			statusQuery.should(boolQueryBuilder);
			statusQuery.should(aQueryBuilder);
			allQuery.must(statusQuery);

			AbstractAggregationBuilder cSum = AggregationBuilders.sum("count").field("count");
			AbstractAggregationBuilder statusKey = AggregationBuilders.terms("status").field("status").subAggregation(cSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder ispKey = AggregationBuilders.terms("isp").field("isp").subAggregation(statusKey).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder prvKey = AggregationBuilders.terms("prv").field("prv").subAggregation(ispKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(prvKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms prvAgge = response.getAggregations().get("prv");
			for (Bucket bucket : prvAgge.getBuckets()) {
				StringTerms ispAgge = bucket.getAggregations().get("isp");
				Map<String, Map<String, Long>> ispMap = new TreeMap<String, Map<String, Long>>();
				for (Bucket ispBucket : ispAgge.getBuckets()) {
					StringTerms ratioAgge = ispBucket.getAggregations().get("status");
					Map<String, Long> map = new TreeMap<String, Long>();
					for (Bucket sta : ratioAgge.getBuckets()) {
						String key = sta.getKey();
						Sum count = sta.getAggregations().get("count");
						long value = ((Double) count.getValue()).longValue();
						map.put(key, value);
					}
					ispMap.put(ispBucket.getKey(), map);
				}
				result_.put(bucket.getKey(), ispMap);
			}
		} catch (RuntimeException e) {
			this.message = "请求失败";
			log.error("Query Fail: " + e);
			return 0;
		} finally {
			if (client != null) {
				client.close();
			}
		}
		return 1;
	}

	@Override
	public void validate() {
		super.validate();
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
		}
	}
	
	public List<Map<String, String>> getResult() {
		for(Entry<String, Map<String, Map<String, Long>>> prvEntry : result_.entrySet()){
			Map<String, Map<String, Long>> prvMap = prvEntry.getValue();
			for(Entry<String, Map<String, Long>> ispEntry : prvMap.entrySet()){
				Map<String, String> map = new TreeMap<String,String>();
				Map<String, Long> ispMap = ispEntry.getValue();
				Long long0 = ispMap.containsKey("0") ? ispMap.get("0") : 0L;
				Long long1 = ispMap.containsKey("1") ? ispMap.get("1") : 0L;
				map.put("0", String.valueOf(long0));
				map.put("1", String.valueOf(long1));
				map.put("prv", prvEntry.getKey());
				map.put("isp", ispEntry.getKey());
				result.add(map);
			}
		}
		return result;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public String getMessage() {
		return message;
	}

	public void setPrv(String prv) {
		this.prv = prv;
	}

	public void setIsp(String isp) {
		this.isp = isp;
	}
}
