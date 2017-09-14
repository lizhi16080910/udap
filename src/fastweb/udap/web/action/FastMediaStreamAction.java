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

import fastweb.udap.web.EsClientFactory;

/**
 * 流媒体直播在线流数统计
 */
@Namespace("/")
@Action(value = "fastmediastream", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastMediaStreamAction extends ActionSupport {

	private final Log log = LogFactory.getLog(getClass());
	public static final String INDEX = "fastmedia_stream_count";
	private static final long serialVersionUID = 1L;
	private String domain;
	private String platform;
	private String app;
	private long begin;
	private long end;
	private Map<String, Map<String, Long>> result = new TreeMap<String, Map<String, Long>>();
	private String message;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + platform + "\t" + app + "\t" + domain);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("timestamp").from(begin).to(end)).must(QueryBuilders.termsQuery("domain", this.domain.split(",")));

			if (StringUtils.isNotBlank(app)) {
				QueryBuilder appQuery = QueryBuilders.termQuery("app", this.app);
				allQuery.must(appQuery);
			}
			
			if (StringUtils.isNotBlank(platform)) {
				QueryBuilder platformQuery = QueryBuilders.queryString("*" + this.platform + "*").field("platform");
				allQuery.must(platformQuery);

			}
			
			AbstractAggregationBuilder cSum = AggregationBuilders.sum("stream_count").field("stream_count");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(cSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder domainKey = AggregationBuilders.terms("domain").field("domain").subAggregation(timestampKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(domainKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();
			StringTerms domainTerms = response.getAggregations().get("domain");
			for (Bucket domainBucket : domainTerms.getBuckets()) {
				StringTerms timestampTerms = domainBucket.getAggregations().get("timestamp");
				Map<String, Long> maps = new TreeMap<String, Long>();
				for (Bucket timestampBucket : timestampTerms.getBuckets()) {
					Sum sum = timestampBucket.getAggregations().get("stream_count");
					String key = timestampBucket.getKey();
					long value = ((Double) sum.getValue()).longValue();
					maps.put(key, value);
				}
				result.put(domainBucket.getKey(), maps);
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
		if (StringUtils.isBlank(domain)) {
			setMessage("域名不能为空。");
		}
		if (begin > end) {
			setMessage("开始时间戳大于结束时间戳");
		}
	}

	public Map<String, Map<String, Long>> getResult() {
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

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setApp(String app) {
		this.app = app;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}
}
