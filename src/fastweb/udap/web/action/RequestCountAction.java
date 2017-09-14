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
import fastweb.udap.util.Time2Type;
import fastweb.udap.web.EsClientFactory;

/**
 * @author yecg
 */
@Namespace("/")
@Action(value = "requestcount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
		   @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class RequestCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;
	/*index 前缀名*/
	static private String indexPrefix = "cdnlog.flow.size.count";
	/*metric*/
	private String metric = MetricConst.DOMAIN_REQ_COUNT_ONE.getMetricStr();
	/* 域名*/
	private String domain;
	/* 查询开始时间戳*/
	private long begin;
	/* 查询结束时间戳*/
	private long end;
	/* userid */
	private String userid = null;
	/* 省份 */
	private String prv = null;
	
	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
	
	private String message;
	
	@Override
	public String execute() throws Exception {
		if(!StringUtil.isEmpty(message)) {
			return ActionSupport.ERROR;
		}
		try {
			Client client = EsClientFactory.createClient();
			try {
				/* construct query */
				QueryBuilder timeStampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timeStampQuery);
			
				/* when domain equals "all", use userid to search for all domains thats owned by this userid */
				if( domain.equalsIgnoreCase("all") && null != userid) {
					allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
				} else{
					allQuery = allQuery.must(QueryBuilders.queryString(this.domain).field("domain"));
				}
				
				if(metric.startsWith("domain.req.prv.count") && null != prv && !prv.equalsIgnoreCase("all")){ /*按城市分组来查询*/
					allQuery = allQuery.must(QueryBuilders.termQuery("prv",this.prv));
				}
			
				/*index 存在性检*/
				String[] existsIndices = ESUtil.getExistIndices(getIndicesByMetric()).toArray( new String[0]);
				if( 0 ==  existsIndices.length) { /*不存在index*/
					setMessage("No index for this time span exists!");
					return ActionSupport.ERROR;
				}
				
				SearchRequestBuilder searchBuilder = client.prepareSearch(existsIndices)
						.setQuery(allQuery);

				/* 构建 aggregation */
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
			e.printStackTrace();
		} finally {
		}
		return ActionSupport.SUCCESS;
	}
	
	private List<String> getOfflineIndices()
	{
		String [] metricParts = metric.split("\\.");
		List<String> indicesSuffix = Time2Type.time2Indices(begin, end);
		List<String> indices = new ArrayList<String>();
	
		for(String index : indicesSuffix){
			indices.add(indexPrefix+"."+metricParts[metricParts.length-1]+"."+index);
		}
		/* add timeout index */
		indices.add(indexPrefix+"."+metricParts[metricParts.length-1]);
		return indices;
	}
	
	private List<String> getOnlineIndices()
	{
		List<String> indices = new ArrayList<String>();
		indices.add(indexPrefix);
		return indices;
	}
	
	private List<String> getIndicesByMetric()
	{
		if(metric.endsWith("one")){
			return getOnlineIndices();
		} else {
			return getOfflineIndices();
		}
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
	
	private String getMetricFieldByMetric() {
		return "req_count";
	}
	
	private String getAggFieldByMetric() {
		if(metric.startsWith("domain.req.count")) {
			return "timestamp";
		} else if(metric.startsWith("domain.req.hit.count")) {
			return "hit_type";
		} else if(metric.startsWith("domain.req.prv.count")){ 
			if(null == prv ) {
				return "prv";
			} else{
				return "city";
			}
		}  else if(metric.startsWith("domain.req.isp.count"))
		{
			return "isp";
		} else {
			return "hit_type";
		}
	}	
	/**
	 * @param domain the domain to set
	 */
	public void setDomain(String domain) {
		this.domain = domain;
	}
	
	/**
	 * @param begin the begin to set
	 */
	public void setBegin(long begin) {
		this.begin = begin;
	}
	
	/**
	 * @param end 
	 * set the search end timestamp
	 */
	public void setEnd(long end) {
		this.end = end;
	}
	
	/**
	 * @param metric 
	 * set metric for elasticsearch
	 */
	public void setMetric(String metric) {
		this.metric = metric;
	}

	
	public List<Map<String, Object>> getResult() {
		return result;
	}
	
	
	public void setUserid(String userid) {
		this.userid = userid;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	public void setPrv(String prv) {
		this.prv = prv;
	}
	
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
		DOMAIN_REQ_COUNT_ONE("domain.req.count.one"), DOMAIN_REQ_COUNT_FIVE("domain.req.count.five"), DOMAIN_REQ_COUNT_HOUR("domain.req.count.hour"),/*按时间统计*/
		DOMAIN_REQ_HIT_COUNT_ONE("domain.req.hit.count.one"), DOMAIN_REQ_HIT_COUNT_FIVE("domain.req.hit.count.five"), DOMAIN_REQ_HIT_COUNT_HOUR("domain.req.hit.count.hour"),/*按回源情况统计*/
		DOMAIN_REQ_PRV_COUNT_ONE("domain.req.prv.count.one"), DOMAIN_REQ_PRV_COUNT_FIVE("domain.req.prv.count.five"), DOMAIN_REQ_PRV_COUNT_HOUR("domain.req.prv.count.hour"),/*按地区统计*/
		DOMAIN_REQ_ISP_COUNT_ONE("domain.req.isp.count.one"), DOMAIN_REQ_ISP_COUNT_FIVE("domain.req.isp.count.five"), DOMAIN_REQ_ISP_COUNT_HOUR("domain.req.isp.count.hour");/*按ISP统计请求数*/
		
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

