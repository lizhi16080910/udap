package fastweb.udap.web.action;

import java.util.Arrays;
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
 * 熊猫tv在线人数统计
 */
@Namespace("/")
@Action(value = "pandaonline", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class PandaOnlineAction extends ActionSupport {
	
	private final Log log = LogFactory.getLog(getClass());
	public static final String INDEX = "pandatv_online";
	private static final long serialVersionUID = 1L;
	private String domain;
	private String stream;
	private String platform;
	private String app;
	private long begin;
	private long end;
	private Map<String, Map<String, Long>> result = new TreeMap<String, Map<String, Long>>();
	private String message;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + platform + "\t" + app + "\t" + stream + "\t" + domain);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
			QueryBuilder domainQuery = QueryBuilders.termQuery("domain", this.domain);
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery).must(domainQuery);
			if (StringUtils.isNotBlank(app)) {
				QueryBuilder appQuery = QueryBuilders.termQuery("app", this.app);
				allQuery.must(appQuery);
			}
			if (StringUtils.isNotBlank(stream)) {
				QueryBuilder streamQuery = QueryBuilders.termQuery("stream", this.stream);
				allQuery.must(streamQuery);
			}
			if (StringUtils.isNotBlank(platform)) {

				String p = "*";
				String[] platforms = platform.split(",");
				Arrays.sort(platforms);
				for (int i = 0; i < platforms.length; i++) {
					p += platforms[i] + "*";
				}

				QueryBuilder platformQuery = QueryBuilders.queryString(p).field("platform");
				allQuery.must(platformQuery);

			}
			AbstractAggregationBuilder csSum = AggregationBuilders.sum("nclients").field("nclients");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(csSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder streamKey = AggregationBuilders.terms("stream").field("stream").subAggregation(timestampKey).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder appKey = AggregationBuilders.terms("app").field("app").subAggregation(streamKey).size(Integer.MAX_VALUE);
			
			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(appKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			
			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms appTerms = response.getAggregations().get("app");
			for (Bucket appBucket : appTerms.getBuckets()) {
				String appName = appBucket.getKey();
				StringTerms streamTerms = appBucket.getAggregations().get("stream");
				for (Bucket streamBucket : streamTerms.getBuckets()) {
					String streamName = streamBucket.getKey();
					StringTerms timestampTerms = streamBucket.getAggregations().get("timestamp");

					Map<String, Long> map = new TreeMap<String, Long>();

					for (Bucket timestampBucket : timestampTerms.getBuckets()) {
						Sum sum = timestampBucket.getAggregations().get("nclients");
						String key = timestampBucket.getKey();
						long value = ((Double) sum.getValue()).longValue();
						map.put(key, value);
					}
					result.put(appName + "/" + streamName, map);
				}
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
		if (StringUtil.isEmpty(domain)) {
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

	public void setStream(String stream) {
		this.stream = stream;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}
}
