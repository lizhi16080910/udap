package fastweb.udap.web.action.fastmedia;

import java.util.Map;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * 流媒体数据监控 域名  在线人数   OR 在线主播 按省份 排行top
 */
@Namespace("/")
@Action(value = "fastmediaprvonlinepublishtop", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaPrvOnlinePublishTopAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());
	private Map<String, Map<String, Long>> result = new TreeMap<String, Map<String, Long>>();
	public static final String INDEX = "fastmedia_monitor";
	private String message;
	private String userid;
	private String platform;
	private String hostname;
	private String params;
	private String domain;
	private String prv;
	private long begin = 0;
	private long end = 0;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + platform + "\t" + prv + "\t" + domain + "\t" + hostname + "\t" + userid);
			return ActionSupport.ERROR;
		}
		
		if("0".equals(params)){
			params = "nclients";
		}else{
			params = "publish";
		}
		
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);

			if (StringUtils.isNotBlank(prv) && !"all".equals(prv)) {
				allQuery.must(QueryBuilders.termsQuery("prv", this.prv.split(",")));
			}
			if (StringUtils.isNotBlank(userid)) {
				allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			}
			if (StringUtils.isNotBlank(domain)) {
				allQuery = allQuery.must(QueryBuilders.termQuery("domain", this.domain));
			}
			if (StringUtils.isNotBlank(platform)) {
				QueryBuilder platformQuery = QueryBuilders.queryString("*" + this.platform + "*").field("platform");
				allQuery.must(platformQuery);
			}
			if (StringUtils.isNotBlank(hostname)) {
				allQuery.must(QueryBuilders.termQuery("hostname", this.hostname));
			}

			AbstractAggregationBuilder cSum = AggregationBuilders.sum(params).field(params);
			AbstractAggregationBuilder prvKey = AggregationBuilders.terms("prv").field("prv").subAggregation(cSum).size(20).order(Terms.Order.aggregation(params, false));
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(prvKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(timestampKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms timestampAgge = response.getAggregations().get("timestamp");
			for (Bucket domainBucket : timestampAgge.getBuckets()) {
				StringTerms prvAgge = domainBucket.getAggregations().get("prv");
				Map<String, Long> map = new TreeMap<String, Long>();
				for (Bucket sta : prvAgge.getBuckets()) {
					String key = sta.getKey();
					Sum count = sta.getAggregations().get(params);
					long value = ((Double) count.getValue()).longValue();
					map.put(key, value);
				}
				result.put(domainBucket.getKey(), map);
			}
		} catch (RuntimeException e) {
			this.message = "请求失败";
			log.error("Query Fail: " + e);
			return ActionSupport.ERROR;
		} finally {
			if (client != null) {
				client.close();
			}
		}
		return ActionSupport.SUCCESS;
	}

	@Override
	public void validate() {
		super.validate();
		if (begin == 0 || end == 0 || (end - begin < 0) || (end - begin > 7200)) {
			setMessage("开始时间结束时间不能为空,结束时间要大于等于开始时间,时间跨度不能超过2小时.");
		}
		if (!("0".equals(params) || "1".equals(params))) {
			setMessage("监控参数params(0:在线人数, 1：在线主播)只能从中二选一");
		}
	}

	public Map<String, Map<String, Long>> getResult() {
		return result;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getMessage() {
		return message;
	}

	public void setPrv(String prv) {
		this.prv = prv;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setParams(String params) {
		this.params = params;
	}
}
