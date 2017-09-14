package fastweb.udap.web.action.wangyi;

import java.text.SimpleDateFormat;
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
 * 状态码499 4XX热度分析
 */
@Namespace("/")
@Action(value = "cdnlogstatuscode", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class CdnlogStatusCodeAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private static SimpleDateFormat SD = new SimpleDateFormat("yyyyMMdd");

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "cdnlog.status.code";

	private long begin = 0;
	private long end = 0;
	private String domain;
	private String userid;
	private String message;

	private Map<Long, Map<String, Long>> result = new TreeMap<Long, Map<String, Long>>();

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}

		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {

			client = EsClientFactory.createClient();

			QueryBuilder timeQuery = QueryBuilders.rangeQuery("fivetime").from(begin).to(end);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timeQuery);

			if (StringUtils.isNotBlank(userid)) {
				allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			}

			if (StringUtils.isNotBlank(domain)) {
				allQuery.must(QueryBuilders.inQuery("domain", this.domain.split(",")));
			}
			AbstractAggregationBuilder countSum = AggregationBuilders.sum("count").field("count");
			AbstractAggregationBuilder statusKey = AggregationBuilders.terms("status").field("status").subAggregation(countSum).size(100);
			AbstractAggregationBuilder httphttpsKey = AggregationBuilders.terms("hit").field("hit").subAggregation(statusKey).size(100);
			AbstractAggregationBuilder dayKey = AggregationBuilders.terms("interval").field("interval").subAggregation(httphttpsKey).size(100);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(dayKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms dayAgge = response.getAggregations().get("interval");

			for (Bucket daybucket : dayAgge.getBuckets()) {
				StringTerms httphttpsAgge = daybucket.getAggregations().get("hit");
				Map<String, Long> statusMap = new TreeMap<String, Long>();
				for (Bucket httphttpsbucket : httphttpsAgge.getBuckets()) {
					StringTerms statusAgge = httphttpsbucket.getAggregations().get("status");
					for (Bucket statusbucket : statusAgge.getBuckets()) {
						String key = statusbucket.getKey();
						Sum count = statusbucket.getAggregations().get("count");
						long value = ((Double) count.getValue()).longValue();
						statusMap.put((("0".equals(httphttpsbucket.getKey())) ? "https" : "http") + "_" + key, value);
					}
				}
				result.put(SD.parse(daybucket.getKey()).getTime() / 1000, statusMap);
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
		if (StringUtils.isBlank(domain) && StringUtils.isBlank(userid)) {
			setMessage("参数domain和userid不能全部为空。");
		}
//		if (StringUtils.isNotBlank(schema) && (!("http".equals(schema) || "https".equals(schema)))) {
//			setMessage("参数schema不为空情况,只能为(http或者https)");
//		}
		if (begin == 0 || end == 0 || (end - begin < 0)) {
			setMessage("开始时间和截止时间不能为空。");
		}
	}

	public Map<Long, Map<String, Long>> getResult() {
		return result;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
}
