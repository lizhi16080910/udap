package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.ESUtil;
import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * @author shuzhangyao
 */
@Namespace("/")
@Action(value = "uacount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
									   @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class UaCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private static final String indexPrefix ="cdnlog.ua";

	/* 时间戳开始 */
	private long begin;
	/* 时间戳 结束*/
	private long end;
	/* 域名 */
	private String domain;

	private String message;
	
	private String userid = null;
	
	private String metric = MetricConst.UA_REQ_COUNT_ONE.getMetricStr();

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

	@Override
	public String execute() throws Exception {
		if(!StringUtil.isEmpty(message)){
			return ActionSupport.ERROR;
		} 
		try {

			Client client = EsClientFactory.createClient();
			try {
				QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
				
				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);
				
				if(domain.equalsIgnoreCase("all") && null != userid ) {
					allQuery = allQuery.must(QueryBuilders.termQuery("userid",this.userid));
				} else {
					allQuery = allQuery.must(QueryBuilders.queryString(this.domain).field("domain"));
				}
				/*index 存在性检*/
				String[] existsIndices = ESUtil.getExistIndices(getIndicesByMetric()).toArray( new String[0]);
				if( 0 ==  existsIndices.length) { /*不存在index*/
					setMessage("No index for this time span exists!");
					return ActionSupport.ERROR;
				}
				SearchRequestBuilder searchBuilder = client.prepareSearch(existsIndices).setQuery(allQuery);
				
				/* build the aggregation */
				String metricFieldName = getMetricFieldByMetric();
				AbstractAggregationBuilder metricFieldAgg = AggregationBuilders.sum(metricFieldName).field(metricFieldName);
				String aggFieldName = getAggFieldByMetric();
				SearchResponse searchResponse = searchBuilder.addAggregation(AggregationBuilders.terms(aggFieldName).field(aggFieldName).size(Integer.MAX_VALUE).subAggregation(metricFieldAgg))
						.setFrom(0).setSize(0)
						.execute().actionGet();
				generateFinalResult(searchResponse,0);
				
		
			} finally {
				client.close();
			}
		
		} catch (Exception e) {

		} finally {
		}
		return ActionSupport.SUCCESS;
	}
	
	
	private void generateFinalResult(SearchResponse searchResponse, int type) {
		if(0 == type) {
			String  aggFieldName = getAggFieldByMetric();
	        Terms aggFieldTerm = searchResponse.getAggregations().get(aggFieldName);
	        List<Bucket> bucketList = aggFieldTerm.getBuckets();
	        Map<String,Object> aggMap = new HashMap<String,Object>();
	        List<Object> keyValueList = new ArrayList<Object>();
	        aggMap.put(aggFieldName, keyValueList);
	        Map<String,Long> keyValueTurple = new TreeMap<String,Long>();
	        for(Bucket buck:bucketList) {
				String metricFieldName = getMetricFieldByMetric();
				Sum sumAgg = buck.getAggregations().get(metricFieldName);
				keyValueTurple.put(buck.getKey(),((Double)sumAgg.getValue()).longValue());
			}
	        if(!keyValueTurple.isEmpty()) {
	        	keyValueList.add(keyValueTurple);
	        }
			result.add(aggMap);
		} else {
		    Iterator<SearchHit> it = searchResponse.getHits().iterator();
			while (it.hasNext()) {
				SearchHit sh = it.next();
				result.add(sh.getSource());
			}
		}
	}
	
	private String getAggFieldByMetric() {
		return "ua";
	}


	private String getMetricFieldByMetric() {
		if(metric.startsWith("ua.flow")) {
			return "flow_size";
		} else {
			return "req_count";
		}
	}


	public List<Map<String, Object>> getResult() {
		return result;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	private List<String> getOfflineIndices()
	{
		String [] metricParts = metric.split("\\.");
		List<String> indices = new ArrayList<String>();
		indices.add(indexPrefix+"."+metricParts[metricParts.length-1]);
		return indices;
	}
	
	private List<String> getOnlineIndices()
	{
		List<String> indices = new ArrayList<String>();
		indices.add(indexPrefix);
		return indices;
	}
	
	private List<String> getIndicesByMetric(){
		if(metric.endsWith("one")) {
			return getOnlineIndices();
		} else {
			return getOfflineIndices();
		}
	}
	
	public void setUserid(String userid) {
		this.userid = userid;
	}
	/* check the validity of parameters pass in */
	@Override
	public void validate() {
		super.validate();
		if(StringUtil.isEmpty(domain)){
			setMessage("参数domain为空");
		} 
	
		if(begin > end)
		{
			setMessage("开始时间戳大于结束时间戳");
		}
		
		if(!MetricConst.getAllMetricStr().contains(metric)){
			setMessage("metric参数值不在给定列表中");
		}
	}
	
	/* 请求数查询支持的所有metric*/
	public enum MetricConst {
		UA_REQ_COUNT_ONE("ua.req.count.one"), UA_REQ_COUNT_FIVE("ua.req.count.five"), UA_REQ_COUNT_HOUR("ua.req.count.hour"),/*查询请求数*/
		UA_FLOW_COUNT_ONE("ua.flow.count.one"), UA_FLOW_COUNT_FIVE("ua.flow.count.five"), UA_FLOW_COUNT_HOUR("ua.flow.count.hour");/*查询请求数*/
		
		private String metricStr;
		
		private MetricConst(String metricStr) {
			this.metricStr = metricStr;
		}
		
		private String getMetricStr()
		{
			return this.metricStr;
		}
		
		private static Set<String> getAllMetricStr()
		{
			Set<String> rst = new TreeSet<String>();
			for(MetricConst value:values())
			{
				rst.add(value.getMetricStr());
			}
			return rst;
		}
	}
}
