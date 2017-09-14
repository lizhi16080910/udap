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
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * 查询运营商,省份的访问次数和慢速比的柱状图展示 return->0：慢速(<80) return->1：快速(>=80)
 */
@Namespace("/")
@Action(value = "kscloudspeedcount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class KscloudSpeedCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private Map<String, Map<String, Long>> result = new TreeMap<String, Map<String, Long>>();
	public static final String INDEX = "kscloud_speed";
	private String message;
	private String domain;
	private String userid;
	private String prv;
	private String isp;
	private long begin;
	private long end;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);
			// all查询所有isp数据，参数不为all，查询对应的isp
			if (!"all".equalsIgnoreCase(isp)) {
				QueryBuilder ispQuery = QueryBuilders.termQuery("isp", this.isp);
				allQuery.must(ispQuery);
			}

			// userid和domain都不传，就是查询ES里面的所有域名的情况
			if ("all".equalsIgnoreCase(domain) && userid != null) {
				// 客户userid查询
				allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			} else if (domain != null && !domain.equalsIgnoreCase("all") && userid == null) {
				// 单域名匹配
				allQuery = allQuery.must(QueryBuilders.termQuery("domain", this.domain));
			}
			if (StringUtils.isNotBlank(prv)) {
				QueryBuilder prvQuery = QueryBuilders.termQuery("prv", this.prv);
				allQuery.must(prvQuery);
			}
			BoolQueryBuilder statusQuery = QueryBuilders.boolQuery();
			TermQueryBuilder termQuery = QueryBuilders.termQuery("ratio", "1");
			TermsQueryBuilder aQuery = QueryBuilders.termsQuery("ratio", "0");
			BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(termQuery);
			BoolQueryBuilder aQueryBuilder = QueryBuilders.boolQuery().must(aQuery);
			statusQuery.should(boolQueryBuilder);
			statusQuery.should(aQueryBuilder);
			allQuery.must(statusQuery);

			AbstractAggregationBuilder cSum = AggregationBuilders.sum("count").field("count");
			AbstractAggregationBuilder ratioKey = AggregationBuilders.terms("ratio").field("ratio").subAggregation(cSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder prvKey = AggregationBuilders.terms("prv").field("prv").subAggregation(ratioKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(prvKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();
			StringTerms prvAgge = response.getAggregations().get("prv");
			for (Bucket bucket : prvAgge.getBuckets()) {
				StringTerms ratioAgge = bucket.getAggregations().get("ratio");
				Map<String, Long> map = new TreeMap<String, Long>();
				for (Bucket sta : ratioAgge.getBuckets()) {
					String key = sta.getKey();
					Sum count = sta.getAggregations().get("count");
					long value = ((Double) count.getValue()).longValue();
					map.put(key, value);
				}
				result.put(bucket.getKey(), map);
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
		if (StringUtil.isEmpty(isp)) {
			setMessage("运营商不能为空。");
		}
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
		}
	}

	public Map<String, Map<String, Long>> getResult() {
		return result;
	}

	public void setDomain(String domain) {
		this.domain = domain;
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

	public void setIsp(String isp) {
		this.isp = isp;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}
}
