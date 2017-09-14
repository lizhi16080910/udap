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
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * fastmedia在线主播统计--监控页面使用，没有domain维度
 */
@Namespace("/")
@Action(value = "fastmediaonlinepublish", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaOnlinePublishAction extends ActionSupport {

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "fastmedia_push";
	private static final long serialVersionUID = 1L;
	private String domain;
	private String app;
	private String stream;
	private String platform;
	private long begin;
	private long end;
	private Map<String, Long> result = new TreeMap<String, Long>();
	private String message;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + stream + "\t" + app + "\t" + domain);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);

			if (StringUtils.isNotBlank(domain)) {
				allQuery = allQuery.must(QueryBuilders.termsQuery("domain", this.domain.split(",")));
			}

			if (StringUtils.isNotBlank(stream)) {
				QueryBuilder streamQuery = QueryBuilders.termQuery("stream", this.stream);
				allQuery.must(streamQuery);
			}

			if (StringUtils.isNotBlank(platform)) {
				QueryBuilder platformQuery = QueryBuilders.queryString("*" + this.platform + "*").field("platform");
				allQuery.must(platformQuery);
			}

			if (StringUtils.isNotBlank(app)) {
				QueryBuilder streamQuery = QueryBuilders.termQuery("app", this.app);
				allQuery.must(streamQuery);
			}

			AbstractAggregationBuilder publishSum = AggregationBuilders.sum("publish").field("publish");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(publishSum).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(timestampKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();
			StringTerms timestampTerms = response.getAggregations().get("timestamp");
			for (Bucket timestampBucket : timestampTerms.getBuckets()) {
				Sum sum = timestampBucket.getAggregations().get("publish");
				String key = timestampBucket.getKey();
				long value = ((Double) sum.getValue()).longValue();
				result.put(key, value);
			}
		} catch (RuntimeException e) {
			log.error("Query Fail: " + e);
			this.message = "请求错误";
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
		if (begin > end) {
			setMessage("开始时间戳大于结束时间戳");
		}
	}

	public Map<String, Long> getResult() {
		return result;
	}

	public String getMessage() {
		return message;
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

	public void setStream(String stream) {
		this.stream = stream;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}
}
