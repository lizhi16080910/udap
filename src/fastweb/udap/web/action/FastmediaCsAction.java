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
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * fastmedia总带宽历史信息查询
 */
@Namespace("/")
@Action(value = "fastmediacs", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaCsAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private Map<String, Map<String, Map<String, Long>>> result = new TreeMap<String, Map<String, Map<String, Long>>>();

	private Map<String, String> onlineMap = new TreeMap<String, String>();
	private Map<String, Long> publishMap = new TreeMap<String, Long>();
	private Map<String, Map<String, Long>> csMap = new TreeMap<String, Map<String, Long>>();

	// 推流基本信息查询
	public static final String INDEX_PUBLISH = "fastmedia_push_year";
	// 推流带宽，人数，在播流数据查询
	public static final String INDEX_ONLINE = "pandatv_online_year";
	// 推流带宽
	public static final String INDEX_FASTMEDIA_CS = "fastmedia_cs";

	private String message;
	// 推流域名1:播流域名1;推流域名2:播流域名2--------如:kw1.cdn.dawang.tv:kw2.cdn.dawang.tv;domain1_push:domain1_play
	private String domain;

	private List<String> domain_push_list = new ArrayList<String>();
	private List<String> domain_play_list = new ArrayList<String>();
	private Map<String, String> domains = new TreeMap<String, String>();

	private long begin;
	private long end;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		// 解析域名;
		parseDomain();
		if (initFastMediaCs() == 0) {
			return ActionSupport.ERROR;
		}
		if (initPublishYearBasic() == 0) {
			return ActionSupport.ERROR;
		}
		if (initOnlineYear() == 0) {
			return ActionSupport.ERROR;
		}
		return ActionSupport.SUCCESS;
	}

	// 初始化推流平台fwlog页面数据(f01.i01)查询------上行带宽
	public int initFastMediaCs() {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("timestamp").from(begin).to(end)).must(QueryBuilders.termsQuery("domain", domain_push_list));

			AbstractAggregationBuilder csSum = AggregationBuilders.sum("cs").field("cs");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(csSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder domainKey = AggregationBuilders.terms("domain").field("domain").subAggregation(timestampKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX_FASTMEDIA_CS).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(domainKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms domainAgge = response.getAggregations().get("domain");

			for (Bucket domainBucket : domainAgge.getBuckets()) {
				Map<String, Long> timeMap = new TreeMap<String, Long>();
				StringTerms timeAgge = domainBucket.getAggregations().get("timestamp");
				for (Bucket timeBucket : timeAgge.getBuckets()) {
					Sum sum = timeBucket.getAggregations().get("cs");
					long value = ((Double) sum.getValue()).longValue();
					timeMap.put(timeBucket.getKey(), value);
				}
				csMap.put(domainBucket.getKey(), timeMap);
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

	// 初始化推流平台stat页面数据(f01.i01)查询------主播数
	public int initPublishYearBasic() {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("timestamp").from(begin).to(end)).must(QueryBuilders.termsQuery("domain", domain_push_list));

			AbstractAggregationBuilder publishSum = AggregationBuilders.sum("publish").field("publish");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(publishSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder domainKey = AggregationBuilders.terms("domain").field("domain").subAggregation(timestampKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX_PUBLISH).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(domainKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms domainAgge = response.getAggregations().get("domain");

			for (Bucket domainBucket : domainAgge.getBuckets()) {
				StringTerms timeAgge = domainBucket.getAggregations().get("timestamp");
				for (Bucket timeBucket : timeAgge.getBuckets()) {
					Sum sum = timeBucket.getAggregations().get("publish");
					long value = ((Double) sum.getValue()).longValue();
					publishMap.put(domainBucket.getKey() + "######" + timeBucket.getKey(), value);
				}
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

	// 初始化播流平台stat页面数据(f01.c01)查询------在线人数---下行带宽
	public int initOnlineYear() {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("timestamp").from(begin).to(end)).must(QueryBuilders.termsQuery("domain", domain_play_list));

			AbstractAggregationBuilder countSum = AggregationBuilders.sum("nclients").field("nclients");
			AbstractAggregationBuilder csSum = AggregationBuilders.sum("cs").field("cs");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(countSum).subAggregation(csSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder domainKey = AggregationBuilders.terms("domain").field("domain").subAggregation(timestampKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX_ONLINE).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(domainKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms domainAgge = response.getAggregations().get("domain");

			for (Bucket domainBucket : domainAgge.getBuckets()) {
				StringTerms timeAgge = domainBucket.getAggregations().get("timestamp");
				for (Bucket timeBucket : timeAgge.getBuckets()) {
					Sum cs = timeBucket.getAggregations().get("cs");
					Sum nclients = timeBucket.getAggregations().get("nclients");
					long csValue = ((Double) cs.getValue()).longValue();
					long nclientsValue = ((Double) nclients.getValue()).longValue();
					// 结果统计使用推流域名（播流域名转成推流域名）
					onlineMap.put(domains.get(domainBucket.getKey()) + "######" + timeBucket.getKey(), csValue + "######" + nclientsValue);
				}
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

	public void parseDomain() {
		String[] domain_push_play_array = domain.split(";");
		for (String domain_push_play : domain_push_play_array) {
			String[] push_play = domain_push_play.split(":");
			if (push_play.length == 2) {
				domain_push_list.add(push_play[0]);
				domain_play_list.add(push_play[1]);
				domains.put(push_play[1], push_play[0]);
			}
		}
	}

	// 返回结果
	public Map<String, Map<String, Map<String, Long>>> getResult() {
		for (Entry<String, Map<String, Long>> domainEntry : csMap.entrySet()) {
			Map<String, Map<String, Long>> ts = new TreeMap<String, Map<String, Long>>();
			String domain = domainEntry.getKey();
			Map<String, Long> timeMap = domainEntry.getValue();
			for (Entry<String, Long> timeEntry : timeMap.entrySet()) {
				Map<String, Long> vs = new TreeMap<String, Long>();
				String time = timeEntry.getKey();
				Long cs = timeEntry.getValue();
				vs.put("upcs", cs);
				String uniqueKey = domain + "######" + time;
				if (onlineMap.containsKey(uniqueKey)) {
					vs.put("cs", Long.valueOf(onlineMap.get(uniqueKey).split("######")[0]));
					vs.put("nclients", Long.valueOf(onlineMap.get(uniqueKey).split("######")[1]));
				}
				if (publishMap.containsKey(uniqueKey)) {
					vs.put("publish", publishMap.get(uniqueKey));
				}
				ts.put(time, vs);
			}
			result.put(domain, ts);
		}
		return result;
	}

	@Override
	public void validate() {
		super.validate();
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
		}
		if (StringUtils.isBlank(domain)) {
			setMessage("域名不能为空。");
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

	public void setDomain(String domain) {
		this.domain = domain;
	}
}
