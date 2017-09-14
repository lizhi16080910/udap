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
 * 监控在线人数统计
 */
@Namespace("/")
@Action(value = "fastmediaonline", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaOnlineAction extends ActionSupport {

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "fastmedia_monitor";
	private static final long serialVersionUID = 1L;
	private String domain;
	private String userid;
	private String stream;
	private String platform;
	private String hostname;
	private String iplevel;
	private String prv;
	private String isp;
	private long begin;
	private long end;
	private Map<String, Long> result = new TreeMap<String, Long>();
	private String message;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + platform + "\t" + prv + "\t" + isp + "\t" + hostname + "\t" + stream + "\t" + userid + "\t" + domain);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);

			if ("all".equalsIgnoreCase(domain) && StringUtils.isNotBlank(userid)) {
				// 客户userid查询
				allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			} else if (StringUtils.isNotBlank(domain) && !domain.equalsIgnoreCase("all")) {
				// 单域名匹配
				allQuery = allQuery.must(QueryBuilders.termQuery("domain", this.domain));
			}

			if (StringUtils.isNotBlank(stream)) {
				QueryBuilder streamQuery = QueryBuilders.termQuery("stream", this.stream);
				allQuery.must(streamQuery);
			}

			if (StringUtils.isNotBlank(hostname)) {
				allQuery.must(QueryBuilders.termQuery("hostname", this.hostname));
			}

			if (StringUtils.isNotBlank(isp) && !"all".equals(isp)) {
				allQuery.must(QueryBuilders.termsQuery("isp", this.isp.split(",")));
			}

			if (StringUtils.isNotBlank(prv) && !"all".equals(prv)) {
				allQuery.must(QueryBuilders.termsQuery("prv", this.prv.split(",")));
			}
			
			//1：边缘ip      2：父层ip      3：超父层ip
			if (StringUtils.isNotBlank(iplevel)) {
				allQuery.must(QueryBuilders.termQuery("ip_level", this.iplevel));
			}
			
			if (StringUtils.isNotBlank(platform)) {
				// 接口只传一个平台查询
				QueryBuilder platformQuery = QueryBuilders.queryString("*" + this.platform + "*").field("platform");
				allQuery.must(platformQuery);
				/*String p = "*";
				String[] platforms = platform.split(",");
				Arrays.sort(platforms);
				for(int i=0; i<platforms.length; i++){
					p += platforms[i] + "*";
				}
				QueryBuilder platformQuery = QueryBuilders.queryString(p).field("platform");
				allQuery.must(platformQuery);*/
			}
			AbstractAggregationBuilder csSum = AggregationBuilders.sum("nclients").field("nclients");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(csSum).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(timestampKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms timestampTerms = response.getAggregations().get("timestamp");
			for (Bucket timestampBucket : timestampTerms.getBuckets()) {
				Sum sum = timestampBucket.getAggregations().get("nclients");
				String key = timestampBucket.getKey();
				long value = ((Double) sum.getValue()).longValue();
				result.put(key, value);
			}
		} catch (RuntimeException e) {
			log.error("Query Fail: " + e);
			this.message = "请求错误";
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
		/*if (StringUtils.isBlank(platform)) {
			setMessage("平台不能为空。");
		}*/
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

	public void setPlatform(String platform) {
		this.platform = platform;
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

	public void setHostname(String hostname) {
		this.hostname = hostname;
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

	public void setIplevel(String iplevel) {
		this.iplevel = iplevel;
	}
}
