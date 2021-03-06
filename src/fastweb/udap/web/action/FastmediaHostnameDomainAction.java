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
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * 流媒体数据监控 主机--->域名异常占比
 */
@Namespace("/")
@Action(value = "fastmediahostnamedomain", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaHostnameDomainAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());
	private Map<String, Map<String, Long>> result = new TreeMap<String, Map<String, Long>>();
	public static final String INDEX = "fastmedia_monitor";
	private String message;
	private String domain;
	private String platform;
	private String hostname;
	private String params;
	private String stream;
	private String iplevel;
	private String prv;
	private String isp;
	private long time;

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
			QueryBuilder timestampQuery = QueryBuilders.termQuery("timestamp", this.time);
			QueryBuilder hostnameQuery = QueryBuilders.termQuery("hostname", this.hostname);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery).must(hostnameQuery);

			if (StringUtils.isNotBlank(isp) && !"all".equals(isp)) {
				allQuery.must(QueryBuilders.termsQuery("isp", this.isp.split(",")));
			}
			if (StringUtils.isNotBlank(prv) && !"all".equals(prv)) {
				allQuery.must(QueryBuilders.termsQuery("prv", this.prv.split(",")));
			}
			if (StringUtils.isNotBlank(domain)) {
				allQuery = allQuery.must(QueryBuilders.termQuery("domain", this.domain));
			}

			if (StringUtils.isNotBlank(platform)) {
				QueryBuilder platformQuery = QueryBuilders.queryString("*" + this.platform + "*").field("platform");
				allQuery.must(platformQuery);
			}
			if (StringUtils.isNotBlank(stream)) {
				allQuery.must(QueryBuilders.termQuery("stream", this.stream));
			}

			// 1：边缘ip 2：父层ip 3：超父层ip
			if (StringUtils.isNotBlank(iplevel)) {
				allQuery.must(QueryBuilders.termQuery("ip_level", this.iplevel));
			}

			BoolQueryBuilder statusQuery = QueryBuilders.boolQuery();
			TermQueryBuilder correctQuery = QueryBuilders.termQuery(params, "1");
			TermQueryBuilder errorQuery = QueryBuilders.termQuery(params, "0");
			BoolQueryBuilder correctQueryBuilder = QueryBuilders.boolQuery().must(correctQuery);
			BoolQueryBuilder errorQueryBuilder = QueryBuilders.boolQuery().must(errorQuery);
			statusQuery.should(correctQueryBuilder);
			statusQuery.should(errorQueryBuilder);
			allQuery.must(statusQuery);

			AbstractAggregationBuilder cSum = AggregationBuilders.sum("count").field("count");
			AbstractAggregationBuilder statusKey = AggregationBuilders.terms(params).field(params).subAggregation(cSum);
			AbstractAggregationBuilder domainKey = AggregationBuilders.terms("domain").field("domain").subAggregation(statusKey);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(domainKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms domainAgge = response.getAggregations().get("domain");
			for (Bucket domainBucket : domainAgge.getBuckets()) {
				StringTerms statusAgge = domainBucket.getAggregations().get(params);
				Map<String, Long> params = new TreeMap<String, Long>();
				for (Bucket sta : statusAgge.getBuckets()) {
					String key = sta.getKey();
					Sum count = sta.getAggregations().get("count");
					long value = ((Double) count.getValue()).longValue();
					params.put(key, value);
				}
				result.put(domainBucket.getKey(), params);
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
			setMessage("监控参数不能为空。");
		}
		if (!("fps_status".equals(params) || "buffered_status".equals(params) || "dropped_status".equals(params) || "bit_rate_status".equals(params))) {
			setMessage("监控参数params(fps_status, buffered_status, dropped_status, bit_rate_status)只能从中四选一");
		}
		if (StringUtils.isBlank(hostname)) {
			setMessage("主机名不能为空。");
		}
		if (StringUtils.isBlank(time + "")) {
			setMessage("时间不能为空。");
		}
	}

	public Map<String, Map<String, Long>> getResult() {
		return result;
	}

	public void setTime(long time) {
		this.time = time;
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

	public void setIplevel(String iplevel) {
		this.iplevel = iplevel;
	}

	public void setParams(String params) {
		this.params = params;
	}
}
