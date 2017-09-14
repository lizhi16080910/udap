package fastweb.udap.web.action;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

@Namespace("/")
@Action(value = "tencentparent", results = {
		@Result(name = ActionSupport.SUCCESS, type = "json", params = {
				"excludeProperties", "message" }),
		@Result(name = ActionSupport.ERROR, type = "json", params = {
				"includeProperties", "message" }) })
public class TencentParentAction extends ActionSupport {
	public static final String INDEX = "tencent_parent";
	private static final long serialVersionUID = 1L;
	private String domain;
	private long begin;
	private long end;
	private Map<String, Object> result = new TreeMap<String, Object>();
	private String message;
	/**
	 * 
	 */

	@Override
	public String execute() throws Exception {
		try {
			Client client = EsClientFactory.createClient();
			try {
				String[] domainArray = domain.split(";");
				QueryBuilder domainQuery = QueryBuilders.termsQuery("domain", domainArray);
				QueryBuilder dir1Query;

				QueryBuilder timestampQuery = QueryBuilders.rangeQuery(
						"timestamp").from(begin).to(end);

				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(
						domainQuery).must(timestampQuery);
				
				AbstractAggregationBuilder countSum = AggregationBuilders
						.sum("count").field("count");
				AbstractAggregationBuilder failCountSum = AggregationBuilders
						.sum("failCount").field("fail_count");
				String[] termBucketsName ={"timestamp","domain"};
				
				TermsBuilder domainAgg = AggregationBuilders.terms("dir").field("dir").size(Integer.MAX_VALUE).subAggregation(countSum).subAggregation(failCountSum);
				for(String bucketName : termBucketsName) {
					TermsBuilder termAgg = AggregationBuilders.terms(bucketName).field(bucketName);
					termAgg.subAggregation(domainAgg).size(Integer.MAX_VALUE);
					domainAgg = termAgg;
				}
			
				SearchResponse response = client.prepareSearch(INDEX).setQuery(
						allQuery).setFrom(0).setSize(0).addAggregation(
								domainAgg).execute().actionGet();
				getEsResult(response);
			} finally {
				client.close();
			}
		} catch (Exception e) {
			this.message = "请求错误";
			return ActionSupport.ERROR;
		}

		return ActionSupport.SUCCESS;
	}

	public void getEsResult(SearchResponse response) {
		List<Aggregation> aggList = response.getAggregations().asList();
		if( aggList.isEmpty()) {
			
		} else {
			for(Aggregation agg : aggList){
				result.put(agg.getName(), getAggResult(agg));
			}
		}
	}
	public  Object getAggResult(Aggregation agg) {
	
			if(agg instanceof Terms) { /* 非 leaf aggregation ,此时返回结果中不需要该agg的名字*/
				List<Bucket> bucketList = ((Terms)agg).getBuckets();
				Map<String,Object> bucketMap = new HashMap<String,Object>();
				for(Bucket buck : bucketList) {
					bucketMap.put(buck.getKey(),getBuckResult(buck));
				}
				return bucketMap;
			} else if(agg instanceof Sum) { /* leaf Aggregation */
				Long sumValue = ((Double)((Sum)agg).getValue()).longValue();		
				return sumValue;
			} else {
				return null;
			}
	}
	
	private  Object getBuckResult(Bucket buck) { /* buck下一定有数据*/
		
		List<Aggregation> aggList = buck.getAggregations().asList();
		if(aggList.size() > 1) {
			Map<String,Object> buckRst = new TreeMap<String,Object>();
			for( Aggregation agg : aggList) {
				buckRst.put(agg.getName(),getAggResult(agg));
			}
			return buckRst;
		} else {
			return getAggResult(aggList.get(0)); /* 只有一个子aggregation */
		}
	}
	
	public Map<String, Object> getResult() {
		return result;
	}

	public String getMessage() {
		return message;
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

}
