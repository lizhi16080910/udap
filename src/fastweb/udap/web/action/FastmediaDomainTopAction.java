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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * 流媒体数据监控 域名人数排行top10
 */
@Namespace("/")
@Action(value = "fastmediadomaintop", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaDomainTopAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());
	private Map<String, Map<String, Long>> result = new TreeMap<String, Map<String, Long>>();
	public static final String INDEX = "fastmedia_monitor";
	private String message;
	private String userid;
	private String domain;
	private String platform;
	private String hostname;
	private String stream;
	private String iplevel;
	private String prv;
	private String isp;
	private long begin;
	private long end;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + platform + "\t" + prv + "\t" + isp + "\t" + hostname + "\t" + stream + "\t" + userid);
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
			if (StringUtils.isNotBlank(userid)) {
				allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			}
			if (StringUtils.isNotBlank(domain)) {
				allQuery = allQuery.must(QueryBuilders.termQuery("domain", this.domain));
			}
			if (StringUtils.isNotBlank(platform)) {
				QueryBuilder platformQuery = QueryBuilders.queryString("*" + this.platform + "*").field("platform");
				allQuery.must(platformQuery);
			}
			if (StringUtils.isNotBlank(hostname)) {
				allQuery.must(QueryBuilders.termQuery("hostname", this.hostname));
			}
			if (StringUtils.isNotBlank(stream)) {
				allQuery.must(QueryBuilders.termQuery("stream", this.stream));
			}

			//1：边缘ip      2：父层ip      3：超父层ip
			if (StringUtils.isNotBlank(iplevel)) {
				allQuery.must(QueryBuilders.termQuery("ip_level", this.iplevel));
			}
			AbstractAggregationBuilder cSum = AggregationBuilders.sum("nclients").field("nclients");
			AbstractAggregationBuilder statusKey = AggregationBuilders.terms("domain").field("domain").subAggregation(cSum).size(10).order(Terms.Order.aggregation("nclients", false));
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(statusKey).size(Integer.MAX_VALUE);
			
			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(timestampKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			
			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms timestampAgge = response.getAggregations().get("timestamp");
			for (Bucket domainBucket : timestampAgge.getBuckets()) {
				StringTerms domainAgge = domainBucket.getAggregations().get("domain");
				Map<String, Long> map = new TreeMap<String, Long>();
				for (Bucket sta : domainAgge.getBuckets()) {
					String key = sta.getKey();
					Sum count = sta.getAggregations().get("nclients");
					long value = ((Double) count.getValue()).longValue();
					map.put(key, value);
				}
				result.put(domainBucket.getKey(), map);
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
		/*if (StringUtils.isBlank(platform)) {
			setMessage("平台不能为空。");
		}*/
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
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

	public void setUserid(String userid) {
		this.userid = userid;
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

	public void setDomain(String domain) {
		this.domain = domain;
	}
}
