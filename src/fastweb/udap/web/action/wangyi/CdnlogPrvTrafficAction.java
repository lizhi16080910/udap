package fastweb.udap.web.action.wangyi;

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

import fastweb.udap.web.EsClientFactory;

/**
 * 运营商统计：返回值流量以及占比
 */
@Namespace("/")
@Action(value = "cdnlogprvtraffic", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class CdnlogPrvTrafficAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "cdnlog.isp.prv.traffic";

	/* 时间戳 */
	private long time;
	/* 域名 */
	private String domain;
	/* 客户ID */
	private String userid;

	private String message;
	
	private Map<String, Long> result = new TreeMap<String, Long>();

	@Override
	public String execute() throws Exception {
		if(!StringUtils.isBlank(message)){
			return ActionSupport.ERROR;
		}
		
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {

			client = EsClientFactory.createClient();

			QueryBuilder timeQuery = QueryBuilders.termQuery("time", this.time);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timeQuery);

			if ("all".equalsIgnoreCase(domain) && StringUtils.isNotBlank(userid)) {
				allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			} else {
				allQuery.must(QueryBuilders.queryString(this.domain).field("domain"));
			}

			AbstractAggregationBuilder trafficSum = AggregationBuilders.sum("traffic").field("traffic");
			AbstractAggregationBuilder prvKey = AggregationBuilders.terms("prv").field("prv").subAggregation(trafficSum);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(prvKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();
			
			StringTerms prvAgge = response.getAggregations().get("prv");

			for (Bucket prvbucket : prvAgge.getBuckets()) {
				String key = prvbucket.getKey();
				Sum traffic = prvbucket.getAggregations().get("traffic");
				long value = ((Double) traffic.getValue()).longValue();
				result.put(key, value);
			}
		} catch (Exception e) {
			log.error("Query Fail: " + e);
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
		if(StringUtils.isBlank(domain) && StringUtils.isBlank(userid)){
			setMessage("参数domain和userid不能全部为空。");
		}
		if(StringUtils.isNotBlank(userid) && !"all".equalsIgnoreCase(domain)){
			setMessage("参数userid查询domain参数必须为all。");
		}
		if(time == 0){
			setMessage("查询时间time参数不能为空。");
		}
		
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public Map<String, Long> getResult() {
		return result;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public void setTime(long time) {
		this.time = time;
	}
}
