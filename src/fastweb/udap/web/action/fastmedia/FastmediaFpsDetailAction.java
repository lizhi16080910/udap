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

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * 流媒体数据监控 帧率 buffered 码率 详情
 */
@Namespace("/")
@Action(value = "fastmediafpsdetail", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaFpsDetailAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private Map<String, Map<String, String>> result = new TreeMap<String, Map<String, String>>();
	public static final String INDEX = "fastmedia_details";
	private String message;
	private String domain;
	private String params;
	private String platform;
	private String hostname;
	private String stream;
	private String rightOrError;
	private String id;
	private String address;
	private long begin;
	private long end;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + params + "\t" + platform + "\t" + hostname + "\t" + stream + "\t" + domain);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();

			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);
			allQuery.must(QueryBuilders.queryString("*" + this.platform + "*").field("platform"));
			allQuery.must(QueryBuilders.termQuery("hostname", this.hostname));
			allQuery.must(QueryBuilders.termQuery("stream", this.stream));
			allQuery.must(QueryBuilders.termQuery("domain", this.domain));

			if (StringUtils.isNotBlank(rightOrError) && "fps".equals(params)) {
				allQuery.must(QueryBuilders.termQuery("fps_status", rightOrError));
			} else if (StringUtils.isNotBlank(rightOrError) && "bit_rate".equals(params)) {
				allQuery.must(QueryBuilders.termQuery("bit_rate_status", rightOrError));
			} else if (StringUtils.isNotBlank(rightOrError) && "buffered".equals(params)) {
				allQuery.must(QueryBuilders.termQuery("buffered_status", rightOrError));
			}

			if(StringUtils.isNotBlank(id)){
				allQuery.must(QueryBuilders.termQuery("id", this.id));
			}
			
			if(StringUtils.isNotBlank(address)){
				allQuery.must(QueryBuilders.termQuery("address", this.address));
			}
			
			AbstractAggregationBuilder fpsSum = AggregationBuilders.terms(params).field(params).size(1);
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(fpsSum).size(200);
			AbstractAggregationBuilder idKey = AggregationBuilders.terms("id").field("id").subAggregation(timestampKey).size(100);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(idKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms idAgge = response.getAggregations().get("id");

			for (Bucket idbucket : idAgge.getBuckets()) {
				Map<String, String> idMap = new TreeMap<String, String>();
				StringTerms timeAgge = idbucket.getAggregations().get("timestamp");
				for (Bucket timeBucket : timeAgge.getBuckets()) {
					String key = timeBucket.getKey();
					String value = ((StringTerms) timeBucket.getAggregations().get(params)).getBuckets().get(0).getKey();
					idMap.put(key, value);
				}
				result.put(idbucket.getKey(), idMap);
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
		if (StringUtils.isBlank(params)) {
			setMessage("监控参数params不能为空。");
		}
		if (StringUtils.isBlank(platform)) {
			setMessage("平台platform参数不能为空。");
		}
		if (StringUtils.isBlank(domain)) {
			setMessage("域名domain参数不能为空。");
		}
		if (StringUtils.isBlank(hostname)) {
			setMessage("主机名hostname参数不能为空。");
		}
		if (StringUtils.isBlank(stream)) {
			setMessage("流名称stream参数不能为空。");
		}
		if (!("fps".equals(params) || "bit_rate".equals(params) || "buffered".equals(params))) {
			setMessage("监控参数params(fps, bit_rate, buffered)只能从中三选一");
		}
		if (begin == 0 || end == 0 || (end - begin < 0) || (end - begin > 7200)) {
			setMessage("开始时间结束时间不能为空,结束时间要大于等于开始时间,时间跨度不能超过2小时.");
		}
		if (StringUtils.isNotBlank(rightOrError) && (!"1".equals(rightOrError)) && (!"0".equals(rightOrError))) {
			setMessage("rightOrError要么为空，要么(1,0)中二选一。");
		}
	}

	public Map<String, Map<String, String>> getResult() {
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

	public void setParams(String params) {
		this.params = params;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public void setRightOrError(String rightOrError) {
		this.rightOrError = rightOrError;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setAddress(String address) {
		this.address = address;
	}
}
