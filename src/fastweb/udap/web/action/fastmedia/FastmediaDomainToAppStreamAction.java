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
 * 流媒体---域名---app---流名称
 */
@Namespace("/")
@Action(value = "fastmediadomaintoappstream", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaDomainToAppStreamAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());
	private Map<String, Map<String, Long>> result = new TreeMap<String, Map<String, Long>>();
	//推流域名查询
	public static String INDEX = "fastmedia_push";
	
	private String flag;
	private String message;
	//加速机IP
	private String localaddr;
	//clientIP,远端IP
	private String address;
	private String domain;
	private String platform;
	private String hostname;
	private String stream;
	private String prv;
	private String isp;
	private long begin = 0;
	private long end = 0;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + "\t" + platform + "\t" + prv + "\t" + isp + "\t" + hostname + "\t" + stream + "\t" + domain);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);

			if (StringUtils.isNotBlank(isp) && !"all".equals(isp)) {
				allQuery.must(QueryBuilders.termsQuery("isp", this.isp.split(",")));
			}
			if (StringUtils.isNotBlank(prv) && !"all".equals(prv)) {
				allQuery.must(QueryBuilders.termsQuery("prv", this.prv.split(",")));
			}
			if (StringUtils.isNotBlank(domain)) {
				allQuery = allQuery.must(QueryBuilders.termQuery("domain", this.domain));
			}
			if (StringUtils.isNotBlank(hostname)) {
				allQuery = allQuery.must(QueryBuilders.termQuery("hostname", this.hostname));
			}

			if (StringUtils.isNotBlank(platform)) {
				QueryBuilder platformQuery = QueryBuilders.queryString("*" + this.platform + "*").field("platform");
				allQuery.must(platformQuery);
			}

			if (StringUtils.isNotBlank(stream)) {
				allQuery.must(QueryBuilders.termQuery("stream", this.stream));
			}
			
			if(StringUtils.isNotBlank(flag) && "all".equals(flag)){
				INDEX = "fastmedia_details";
				if (StringUtils.isNotBlank(localaddr)) {
					allQuery = allQuery.must(QueryBuilders.termQuery("localaddr", this.localaddr));
				}
				if (StringUtils.isNotBlank(address)) {
					allQuery = allQuery.must(QueryBuilders.termQuery("address", this.address));
				}
			}
			
			AbstractAggregationBuilder cSum = AggregationBuilders.sum("count").field("count");
			AbstractAggregationBuilder streamKey = AggregationBuilders.terms("stream").field("stream").subAggregation(cSum).size(200);
			AbstractAggregationBuilder appKey = AggregationBuilders.terms("app").field("app").subAggregation(streamKey).size(50);
			
			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(appKey);
			log.info("Query INDEX: " + INDEX + "------Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();
			
			StringTerms appAgge = response.getAggregations().get("app");
			for (Bucket appBucket : appAgge.getBuckets()) {
				StringTerms streamAgge = appBucket.getAggregations().get("stream");
				Map<String, Long> params = new TreeMap<String, Long>();
				for (Bucket str : streamAgge.getBuckets()) {
					String key = str.getKey();
					Sum count = str.getAggregations().get("count");
					long value = ((Double) count.getValue()).longValue();
					params.put(key, value);
				}
				result.put(appBucket.getKey(), params);
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
			setMessage("查询域名不能为空。");
		}
		if (begin == 0 || end == 0 || (end - begin < 0)) {
			setMessage("开始时间和截止时间不能为空。");
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

	public void setIsp(String isp) {
		this.isp = isp;
	}

	public void setDomain(String domain) {
		this.domain = domain;
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

	public void setFlag(String flag) {
		this.flag = flag;
	}

	public void setLocaladdr(String localaddr) {
		this.localaddr = localaddr;
	}

	public void setAddress(String address) {
		this.address = address;
	}
}
