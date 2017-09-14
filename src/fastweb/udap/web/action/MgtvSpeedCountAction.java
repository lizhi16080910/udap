package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.Arrays;
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
 * 查询运营商,省份的访问次数和慢速比 return->0：慢速(<386) return->1：快速(>=386)
 */
@Namespace("/")
@Action(value = "mgtvspeedcount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class MgtvSpeedCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());
	//慢速比数据
	private Map<String, Map<String, Map<String, Map<String, Long>>>> result = new TreeMap<String, Map<String, Map<String, Map<String, Long>>>>();
	//回源数据
	private Map<String, Map<String, Map<String, Map<String, Long>>>> result__ = new TreeMap<String, Map<String, Map<String, Map<String, Long>>>>();
	//返回结果
	private List<Map<String, String>> result_ = new ArrayList<Map<String, String>>();
	//慢速比分组index
	public static final String INDEX_SPEED = "cdnlog.hostname.mgtv.speed";
	//回源不分组index
	public static final String INDEX_HIT = "cdnlog.hostname.mgtv.hit";
	private String message;
	private String prv;
	private List<String> prvs;
	private String isp;
	private List<String> isps;
	private long begin;
	private long end;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		prvs = Arrays.asList(prv.split(","));
		isps = Arrays.asList(isp.split(","));
		if (initSpeed() == 0) {
			return ActionSupport.ERROR;
		}
		if (initHit() == 0) {
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

			BoolQueryBuilder statusQuery = QueryBuilders.boolQuery();
			TermQueryBuilder termQuery = QueryBuilders.termQuery("status", "1");
			TermsQueryBuilder aQuery = QueryBuilders.termsQuery("status", "0");
			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(termQuery);
			BoolQueryBuilder aQueryBuilder = QueryBuilders.boolQuery().must(aQuery);
			statusQuery.should(boolQueryBuilder);
			statusQuery.should(aQueryBuilder);
			allQuery.must(statusQuery);

			AbstractAggregationBuilder cSum = AggregationBuilders.sum("count").field("count");
			AbstractAggregationBuilder ratioKey = AggregationBuilders.terms("status").field("status").subAggregation(cSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder hostnameKey = AggregationBuilders.terms("hostname").field("hostname").subAggregation(ratioKey).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder ispKey = AggregationBuilders.terms("isp").field("isp").subAggregation(hostnameKey).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder prvKey = AggregationBuilders.terms("prv").field("prv").subAggregation(ispKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX_SPEED).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(prvKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();
			
			StringTerms prvAgge = response.getAggregations().get("prv");
			for (Bucket bucket : prvAgge.getBuckets()) {

				StringTerms ispAgge = bucket.getAggregations().get("isp");
				Map<String, Map<String, Map<String, Long>>> ispMap = new TreeMap<String, Map<String, Map<String, Long>>>();
				for (Bucket ispBucket : ispAgge.getBuckets()) {

					StringTerms hostnameAgge = ispBucket.getAggregations().get("hostname");

					Map<String, Map<String, Long>> hostnameMap = new TreeMap<String, Map<String, Long>>();
					for (Bucket hostnameBucket : hostnameAgge.getBuckets()) {
						StringTerms ratioAgge = hostnameBucket.getAggregations().get("status");
						Map<String, Long> map = new TreeMap<String, Long>();
						for (Bucket sta : ratioAgge.getBuckets()) {
							String key = sta.getKey();
							Sum count = sta.getAggregations().get("count");
							long value = ((Double) count.getValue()).longValue();
							map.put(key, value);
						}
						hostnameMap.put(hostnameBucket.getKey(), map);
					}
					ispMap.put(ispBucket.getKey(), hostnameMap);
				}
				result.put(bucket.getKey(), ispMap);
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

	// 初始化回源数据
	public int initHit() {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("fivetime").from(begin).to(end));

			BoolQueryBuilder statusQuery = QueryBuilders.boolQuery();
			TermQueryBuilder termQuery = QueryBuilders.termQuery("hit", "1");
			TermsQueryBuilder aQuery = QueryBuilders.termsQuery("hit", "0");
			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(termQuery);
			BoolQueryBuilder aQueryBuilder = QueryBuilders.boolQuery().must(aQuery);
			statusQuery.should(boolQueryBuilder);
			statusQuery.should(aQueryBuilder);
			allQuery.must(statusQuery);

			AbstractAggregationBuilder cSum = AggregationBuilders.sum("count").field("count");
			AbstractAggregationBuilder ratioKey = AggregationBuilders.terms("hit").field("hit").subAggregation(cSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder hostnameKey = AggregationBuilders.terms("hostname").field("hostname").subAggregation(ratioKey).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder ispKey = AggregationBuilders.terms("isp").field("isp").subAggregation(hostnameKey).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder prvKey = AggregationBuilders.terms("prv").field("prv").subAggregation(ispKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX_HIT).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(prvKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();
			
			StringTerms prvAgge = response.getAggregations().get("prv");
			for (Bucket bucket : prvAgge.getBuckets()) {

				StringTerms ispAgge = bucket.getAggregations().get("isp");
				Map<String, Map<String, Map<String, Long>>> ispMap = new TreeMap<String, Map<String, Map<String, Long>>>();
				for (Bucket ispBucket : ispAgge.getBuckets()) {

					StringTerms hostnameAgge = ispBucket.getAggregations().get("hostname");

					Map<String, Map<String, Long>> hostnameMap = new TreeMap<String, Map<String, Long>>();
					for (Bucket hostnameBucket : hostnameAgge.getBuckets()) {
						StringTerms ratioAgge = hostnameBucket.getAggregations().get("hit");
						Map<String, Long> map = new TreeMap<String, Long>();
						for (Bucket sta : ratioAgge.getBuckets()) {
							String key = sta.getKey();
							Sum count = sta.getAggregations().get("count");
							long value = ((Double) count.getValue()).longValue();
							map.put(key, value);
						}
						hostnameMap.put(hostnameBucket.getKey(), map);
					}
					ispMap.put(ispBucket.getKey(), hostnameMap);
				}
				result__.put(bucket.getKey(), ispMap);
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

	// 返回结果
	public List<Map<String, String>> getResult() {
		Map<String, Map<String, Long>> hostMap = hostMap(result);
		Map<String, String> hitMap = parseMap(result__);

		for (Entry<String, Map<String, Map<String, Map<String, Long>>>> prvEntry : result.entrySet()) {
			if (prvs.contains(prvEntry.getKey())) {
				Map<String, Map<String, Map<String, Long>>> ispMap = prvEntry.getValue();
				for (Entry<String, Map<String, Map<String, Long>>> ispEntry : ispMap.entrySet()) {
					if (isps.contains(ispEntry.getKey())) {
						Map<String, Map<String, Long>> hostnameMap = ispEntry.getValue();
						for (Entry<String, Map<String, Long>> hostEntry : hostnameMap.entrySet()) {
							Map<String, String> resMap = new TreeMap<String, String>();
							resMap.put("prv", prvEntry.getKey());
							resMap.put("isp", ispEntry.getKey());
							resMap.put("hostname", hostEntry.getKey());
							long hostname_0 = hostEntry.getValue().containsKey("0") ? (hostEntry.getValue().get("0")) : 0;
							long hostname_1 = hostEntry.getValue().containsKey("1") ? (hostEntry.getValue().get("1")) : 0;
							long hostname_3 = hostMap.get(hostEntry.getKey()).containsKey("0") ? hostMap.get(hostEntry.getKey()).get("0") : 0;
							long hostname_4 = hostMap.get(hostEntry.getKey()).containsKey("1") ? hostMap.get(hostEntry.getKey()).get("1") : 0;
							
							// 该省份，运营商，主机下，慢速状态数据（分组过滤去重）
							resMap.put("1", String.valueOf(hostname_0));
							// 该省份，运营商，主机下，正常数据（分组过滤去重）
							resMap.put("2", String.valueOf(hostname_1));
							
							// 目标主机慢速总访问数量（分组过滤去重）
							resMap.put("4", String.valueOf(hostname_3));
							// 目标主机正常总访问数量（分组过滤去重）
							resMap.put("5", String.valueOf(hostname_4));
							
							// 目标主机总访问数量（不去重）
							String hitCount = hitMap.get(prvEntry.getKey() + "######" + ispEntry.getKey() + "######" + hostEntry.getKey());
							//client_view访问落在该目标主机上回源次数（不去重）
							resMap.put("3", hitCount.split("######")[0]);
							// 目标主机回源总次数（不去重）
							resMap.put("6", hitCount.split("######")[1]);
							// 目标主机不回源总访问数量（不去重）
							resMap.put("7", hitCount.split("######")[3]);
							// client_view访问落在该目标主机上不回源次数（不去重）
							resMap.put("8", hitCount.split("######")[2]);
							result_.add(resMap);
						}
					}
				}
			}
		}
		return result_;
	}

	
	public Map<String, String> parseMap(Map<String, Map<String, Map<String, Map<String, Long>>>> map_) {
		Map<String, String> map = new TreeMap<String, String>();
		Map<String, Map<String, Long>> hostMap = hostMap(map_);
		for (Entry<String, Map<String, Map<String, Map<String, Long>>>> prvEntry : map_.entrySet()) {
			if (prvs.contains(prvEntry.getKey())) {
				Map<String, Map<String, Map<String, Long>>> ispMap = prvEntry.getValue();
				for (Entry<String, Map<String, Map<String, Long>>> ispEntry : ispMap.entrySet()) {
					if (isps.contains(ispEntry.getKey())) {
						Map<String, Map<String, Long>> hostnameMap = ispEntry.getValue();
						for (Entry<String, Map<String, Long>> hostEntry : hostnameMap.entrySet()) {
							long hostname_0 = hostEntry.getValue().containsKey("0") ? (hostEntry.getValue().get("0")) : 0;
							long hostname_1 = hostEntry.getValue().containsKey("1") ? (hostEntry.getValue().get("1")) : 0;
							long hostname_3 = hostMap.get(hostEntry.getKey()).containsKey("0") ? hostMap.get(hostEntry.getKey()).get("0") : 0;
							long hostname_2 = hostMap.get(hostEntry.getKey()).containsKey("1") ? hostMap.get(hostEntry.getKey()).get("1") : 0;
							map.put(prvEntry.getKey() + "######" + ispEntry.getKey() + "######" + hostEntry.getKey(), hostname_0 + "######" + hostname_3 + "######" + hostname_1 + "######" + hostname_2);
						}
					}
				}
			}
		}
		return map;
	}

	
	//主机上总数量
	public Map<String, Map<String, Long>> hostMap(Map<String, Map<String, Map<String, Map<String, Long>>>> map) {
		Map<String, Map<String, Long>> hostnameCount = new TreeMap<String, Map<String, Long>>();
		for (Entry<String, Map<String, Map<String, Map<String, Long>>>> entry : map.entrySet()) {
			Map<String, Map<String, Map<String, Long>>> ispMap = entry.getValue();
			for (Entry<String, Map<String, Map<String, Long>>> ispEntry : ispMap.entrySet()) {
				Map<String, Map<String, Long>> hostnameMap = ispEntry.getValue();
				for (Entry<String, Map<String, Long>> hostEntry : hostnameMap.entrySet()) {
					String hostname = hostEntry.getKey();
					if (hostnameCount.containsKey(hostname)) {
						Map<String, Long> temp = hostnameCount.get(hostname);
						for (Entry<String, Long> statusEntry : hostEntry.getValue().entrySet()) {
							if (temp.containsKey(statusEntry.getKey())) {
								temp.put(statusEntry.getKey(), statusEntry.getValue() + temp.get(statusEntry.getKey()));
							} else {
								temp.put(statusEntry.getKey(), statusEntry.getValue());
							}
						}
						hostnameCount.put(hostname, temp);
					} else {
						Map<String, Long> temp = new TreeMap<String, Long>();
						for (Entry<String, Long> statusEntry : hostEntry.getValue().entrySet()) {
							temp.put(statusEntry.getKey(), statusEntry.getValue());
						}
						hostnameCount.put(hostname, temp);
					}
				}

			}
		}
		return hostnameCount;
	}

	@Override
	public void validate() {
		super.validate();
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
		}
		if (StringUtils.isBlank(prv)) {
			setMessage("区域不能为空。");
		}
		if (StringUtils.isBlank(isp)) {
			setMessage("运营商不能为空。");
		}
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
