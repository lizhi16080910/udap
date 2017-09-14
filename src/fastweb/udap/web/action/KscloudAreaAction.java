package fastweb.udap.web.action;

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
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * 多域名,多运营商,多区域的带宽面积图(五分钟一个点);
 */
@Namespace("/")
@Action(value = "kscloudarea", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class KscloudAreaAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());
	
	private Map<String, Long> result = new TreeMap<String, Long>();
	public static final String INDEX = "kscloud_isp_prv_cs";
	private String message;
	private String domains;
	private String userid;
	private String prvs;
	private String isps;
	private long begin;
	private long end;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + domains + "\t" + userid + "\t" + prvs + "\t" + isps);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);

			if (StringUtils.isNotBlank(isps)) {
				QueryBuilder ispQuery = QueryBuilders.termsQuery("isp", isps.split(","));
				allQuery.must(ispQuery);
			}
			if (StringUtils.isNotBlank(prvs)) {
				QueryBuilder prvQuery = QueryBuilders.termsQuery("prv", prvs.split(","));
				allQuery.must(prvQuery);
			}

			// userid和domain都不传，就是查询ES里面的所有域名的情况
			if ("all".equalsIgnoreCase(domains) && userid != null) {
				// 客户userid查询
				allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			} else if (domains != null && !domains.equalsIgnoreCase("all") && userid == null) {
				// 域名匹配(多和单)
				allQuery = allQuery.must(QueryBuilders.termsQuery("domain", domains.split(",")));
			}
			AbstractAggregationBuilder cSum = AggregationBuilders.sum("cs").field("cs");
			AbstractAggregationBuilder fivetimeKey = AggregationBuilders.terms("fivetime").field("fivetime").subAggregation(cSum).size(Integer.MAX_VALUE);
			
			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(fivetimeKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			
			SearchResponse response = searchRequestBuilder.execute().actionGet();
			StringTerms fivetimeAgge = response.getAggregations().get("fivetime");
			for (Bucket bucket : fivetimeAgge.getBuckets()) {
				String key = bucket.getKey();
				Sum sum = bucket.getAggregations().get("cs");
				long value = ((Double) sum.getValue()).longValue();
				result.put(key, value);
			}
		} catch (RuntimeException e) {
			this.message = "请求失败";
			log.error("Query Fail: " + e);
			return ActionSupport.ERROR;
		} finally {
			if(client != null){
				client.close();
			}
		}
		return ActionSupport.SUCCESS;
	}

	@Override
	public void validate() {
		super.validate();
		if (StringUtil.isEmpty(isps)) {
			setMessage("运营商不能为空。");
		}
		if (StringUtil.isEmpty(prvs)) {
			setMessage("区域不能为空。");
		}
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
		}
	}

	public Map<String, Long> getResult() {
		return result;
	}

	public void setDomains(String domains) {
		this.domains = domains;
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

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public void setPrvs(String prvs) {
		this.prvs = prvs;
	}

	public void setIsps(String isps) {
		this.isps = isps;
	}
}
