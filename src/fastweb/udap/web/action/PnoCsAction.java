package fastweb.udap.web.action;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.xwork.StringUtils;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
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

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

@Namespace("/")
@Action(value = "pnocs", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class PnoCsAction extends ActionSupport {
	
	public static final String INDEX = "pnocs";
	private static final long serialVersionUID = 1L;
	/*www.hunantv.com;www.fastweb.com.cn;www.163.com*/
	private String domains;
	private long begin;
	private long end;
	private Map<String, Map<String,String>> result = new TreeMap<String, Map<String,String>>();
	private String message;

	@Override
	public String execute() throws Exception {
		if(!StringUtils.isBlank(message)){
			return ActionSupport.ERROR;
		}
		try {
			Client client = EsClientFactory.createClient();
			try {
				QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);
				String[] domainArray = domains.split(";");
				BoolQueryBuilder domAndPnoQuery = QueryBuilders.boolQuery();
				for (String domain : domainArray) {
					TermQueryBuilder termQuery = QueryBuilders.termQuery("domain", domain);
					BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(termQuery);
					domAndPnoQuery.should(boolQueryBuilder);
				}
				allQuery.must(domAndPnoQuery);

				AbstractAggregationBuilder csSum = AggregationBuilders.sum("cs").field("cs");
				AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("fivetime").field("fivetime").subAggregation(csSum).size(Integer.MAX_VALUE);
				AbstractAggregationBuilder domainKey = AggregationBuilders.terms("domain").field("domain").subAggregation(timestampKey).size(Integer.MAX_VALUE);
				SearchResponse response = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(domainKey).execute().actionGet();
				
				StringTerms domainTerms = response.getAggregations().get("domain");
				for(Bucket domainBucket : domainTerms.getBuckets()){
					String domain = domainBucket.getKey();
					Map<String,String> timeMap = new TreeMap<String,String>();
					StringTerms agge = domainBucket.getAggregations().get("fivetime");
					for (Bucket bucket : agge.getBuckets()) {
						Sum sum = bucket.getAggregations().get("cs");
						String key = bucket.getKey();
						double value = sum.getValue();
						timeMap.put(key, String.valueOf(value*8/1024/1024/300));
					}
					result.put(domain, timeMap);
				}
			}catch(RuntimeException e){
				e.printStackTrace();
			} finally {
				client.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			this.message = "请求错误";
			return ActionSupport.ERROR;
		}
		return ActionSupport.SUCCESS;
	}

	@Override
	public void validate() {
		super.validate();
		if(StringUtil.isEmpty(domains)){
			setMessage("参数domain为空");
		}
		if(begin > end){
			setMessage("开始时间戳大于结束时间戳");
		}
	}
	
	public Map<String, Map<String,String>> getResult() {
		String[] domainPnoPairs = domains.split(";");
		for(String domainPnoPair : domainPnoPairs){
			String[] domainPnos = domainPnoPair.split(":");
			String domain = domainPnos[0];
			if(!result.containsKey(domain)){
				Map<String,String> domainMap = new TreeMap<String,String>();
				result.put(domain, domainMap);
			}
		}
		return result;
	}

	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public void setDomains(String domains) {
		this.domains = domains;
	}
	public void setBegin(long begin) {
		this.begin = begin;
	}
	public void setEnd(long end) {
		this.end = end;
	}
}
