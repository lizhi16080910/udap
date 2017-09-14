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
 * 流媒体数据监控(省份->运营商->机器名->主机名->平台)详情
 */
@Namespace("/")
@Action(value = "fastmediamonitorprvispmachine", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaMonitorPrvIspMachineAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Long>>>>>> result = new TreeMap<String, Map<String, Map<String, Map<String, Map<String, Map<String, Long>>>>>>();
	public static final String INDEX = "fastmedia_monitor";
	private String message;
	private String domain;
	private String params;
	private String platform;
	private String hostname;
	private String stream;
	private String userid;
	private String iplevel;
	private String isp;
	private String prv;
	private long begin;
	private long end;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + params + "\t" + platform + "\t" + hostname + "\t" + stream + "\t" + userid + "\t" + domain);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);
			
			//区域--省份
			if (StringUtils.isNotBlank(prv)) {
				QueryBuilder prvQuery = QueryBuilders.termsQuery("prv", this.prv.split(","));
				allQuery.must(prvQuery);
			}
			
			//运营商
			if (StringUtils.isNotBlank(isp)) {
				QueryBuilder ispQuery = QueryBuilders.termsQuery("isp", this.isp.split(","));
				allQuery.must(ispQuery);
			}
			
			//平台
			if (StringUtils.isNotBlank(platform)) {
				QueryBuilder platformQuery = QueryBuilders.queryString("*" + this.platform + "*").field("platform");
				allQuery.must(platformQuery);
			}
			//主机名
			if (StringUtils.isNotBlank(hostname)) {
				allQuery.must(QueryBuilders.termQuery("hostname", this.hostname));
			}
			//流名称
			if (StringUtils.isNotBlank(stream)) {
				allQuery.must(QueryBuilders.termQuery("stream", this.stream));
			}
			//1：边缘ip      2：父层ip      3：超父层ip
			if (StringUtils.isNotBlank(iplevel)) {
				allQuery.must(QueryBuilders.termQuery("ip_level", this.iplevel));
			}
			// userid和domain都不传，就是查询ES里面的所有域名的情况
			if ("all".equalsIgnoreCase(domain) && StringUtils.isNotBlank(userid)) {
				// 客户userid查询
				allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			} else if (StringUtils.isNotBlank(domain) && !domain.equalsIgnoreCase("all")) {
				// 单域名匹配
				allQuery = allQuery.must(QueryBuilders.termQuery("domain", this.domain));
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
			AbstractAggregationBuilder statusKey = AggregationBuilders.terms(params).field(params).subAggregation(cSum).size(10);
			AbstractAggregationBuilder platformKey = AggregationBuilders.terms("platform").field("platform").subAggregation(statusKey).size(10000);
			AbstractAggregationBuilder hostnameKey = AggregationBuilders.terms("hostname").field("hostname").subAggregation(platformKey).size(10000);
			AbstractAggregationBuilder machineKey = AggregationBuilders.terms("machine").field("machine").subAggregation(hostnameKey).size(10000);
			AbstractAggregationBuilder ispKey = AggregationBuilders.terms("isp").field("isp").subAggregation(machineKey).size(50);
			AbstractAggregationBuilder prvKey = AggregationBuilders.terms("prv").field("prv").subAggregation(ispKey).size(100);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(prvKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			SearchResponse response = searchRequestBuilder.execute().actionGet();
			
			StringTerms prvAgge = response.getAggregations().get("prv");
			
			for(Bucket prvbucket : prvAgge.getBuckets()){
				StringTerms ispAgge = prvbucket.getAggregations().get("isp");
				Map<String, Map<String, Map<String, Map<String, Map<String, Long>>>>> ispmap = new TreeMap<String, Map<String, Map<String, Map<String, Map<String, Long>>>>>();
				for (Bucket bucket : ispAgge.getBuckets()) {
					StringTerms machineAgge = bucket.getAggregations().get("machine");
					Map<String, Map<String, Map<String, Map<String, Long>>>> machinemap = new TreeMap<String, Map<String, Map<String, Map<String, Long>>>>();
					for(Bucket machineBucket : machineAgge.getBuckets()){
						StringTerms hostnameAgge = machineBucket.getAggregations().get("hostname");
						Map<String, Map<String, Map<String, Long>>> hostnamemap = new TreeMap<String, Map<String, Map<String, Long>>>();
						for (Bucket hostnameBucket : hostnameAgge.getBuckets()) {
						
							StringTerms platformAgge = hostnameBucket.getAggregations().get("platform");
							Map<String, Map<String, Long>> platformmap = new TreeMap<String, Map<String, Long>>();
							for(Bucket platformBucket : platformAgge.getBuckets()){
								StringTerms statusAgge = platformBucket.getAggregations().get(params);
								Map<String, Long> map = new TreeMap<String, Long>();
								for (Bucket sta : statusAgge.getBuckets()) {
									String key = sta.getKey();
									Sum count = sta.getAggregations().get("count");
									long value = ((Double) count.getValue()).longValue();
									map.put(key, value);
								}
								platformmap.put(platformBucket.getKey(), map);
							}
							hostnamemap.put(hostnameBucket.getKey(), platformmap);
						}
						machinemap.put(machineBucket.getKey(), hostnamemap);
					}
					ispmap.put(bucket.getKey(), machinemap);
				}
				result.put(prvbucket.getKey(), ispmap);
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
		if (StringUtils.isBlank(params)) {
			setMessage("监控参数不能为空。");
		}
		if (!("fps_status".equals(params) || "buffered_status".equals(params) || "dropped_status".equals(params) || "bit_rate_status".equals(params))) {
			setMessage("监控参数params(fps_status, buffered_status, dropped_status, bit_rate_status)只能从中四选一");
		}
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
		}
	}

	public Map<String, Map<String, Map<String, Map<String, Map<String, Map<String, Long>>>>>> getResult() {
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

	public void setPrv(String prv) {
		this.prv = prv;
	}

	public void setUserid(String userid) {
		this.userid = userid;
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

	public void setIplevel(String iplevel) {
		this.iplevel = iplevel;
	}

	public void setIsp(String isp) {
		this.isp = isp;
	}
}
