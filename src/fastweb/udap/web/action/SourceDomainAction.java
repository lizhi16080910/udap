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
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.ESUtil;
import fastweb.udap.util.StringUtil;
import fastweb.udap.util.Time2Type;
import fastweb.udap.web.EsClientFactory;

/**
 * 回源
 * 
 * @author shuzhangyao
 */
@Namespace("/")
@Action(value = "sourcedomain", results = {@Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
	   									  @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class SourceDomainAction extends ActionSupport {

	private static final long serialVersionUID = 1L;
	
	/*index 前缀名*/
	static private String indexPrefix = "cdnlog.flow.size.count";
	
	

	/* 时间戳 */
//	private long timestamp;
	
	/*时间戳开始*/
	private long begin;
	/*时间戳结束*/
	private long end;
	/* 域名 */
	private String domain;

	/* metric */
	private String metric = MetricConst.SOURCE_DOMIMAIN_ONE.getMetricStr();
	
	private int hitType = 2;

	private String userid = null;

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
				QueryBuilder hitTypeQuery = QueryBuilders.termQuery("hit_type", this.hitType);
				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timeStampQuery).must(hitTypeQuery);
				
				
				/* when domain equals "all", use userid to search for all domains thats owned by this userid */
				if( domain.equalsIgnoreCase("all") && null != userid) {
					allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
				} else{
					allQuery = allQuery.must(QueryBuilders.queryString(this.domain).field("domain"));
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
				String[] metricFieldNames = getMetricFieldByMetric();
				String aggFieldName = getAggFieldByMetric();
				TermsBuilder  termBuilder = AggregationBuilders.terms(aggFieldName).field(aggFieldName).size(Integer.MAX_VALUE);
				for(String fieldName : metricFieldNames){
					termBuilder.subAggregation(AggregationBuilders.sum(fieldName).field(fieldName));
				}
				SearchResponse searchResponse = searchBuilder.addAggregation(termBuilder)
							.setFrom(0).setSize(0)
							.execute().actionGet();
				generateFinalResult(searchResponse,0);
				
			} finally {
				client.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ActionSupport.SUCCESS;
	}

	public void setUserid(String userid) {
		this.userid = userid;
	}

	private void generateFinalResult(SearchResponse searchResponse, int type) {
		if(0 == type) {
			String  aggFieldName = getAggFieldByMetric();
	        Terms aggFieldTerm = searchResponse.getAggregations().get(aggFieldName);
	        List<Bucket> bucketList = aggFieldTerm.getBuckets();
	        Map<String,Object> aggMap = new HashMap<String,Object>();
	        List<Object> keyValueList = new ArrayList<Object>();
	        aggMap.put(aggFieldName, keyValueList);
	        Map<String,Object> keyValueMap = new TreeMap<String,Object>();
	        for(Bucket buck:bucketList) {
	       
				String[] metricFieldNames = getMetricFieldByMetric();
				Map<String,Object> buckRst = new TreeMap<String,Object>();
				for(String fieldName : metricFieldNames) {
			         
					if(fieldName.equalsIgnoreCase("speed_size")) {
						Sum sumAgg1 = buck.getAggregations().get(fieldName);
						Long speedSize = ((Double)sumAgg1.getValue()).longValue();
						Sum sumAgg2 = buck.getAggregations().get("req_count");
						Long reqCount = ((Double)sumAgg2.getValue()).longValue();
						Sum sumAgg3 = buck.getAggregations().get("zeroSpeedReqNum");
						Long zeroSpeedReqNum = ((Double)sumAgg3.getValue()).longValue();
						if( reqCount.equals(zeroSpeedReqNum)) {
							if( 0 != speedSize ) {
								buckRst.put("avg_speed",Integer.MAX_VALUE);
							}
						} else {
							buckRst.put("avg_speed",speedSize/(reqCount-zeroSpeedReqNum));
						}
						
					} else {
						Sum sumAgg = buck.getAggregations().get(fieldName);
						buckRst.put(sumAgg.getName(), ((Double)sumAgg.getValue()).longValue());
					}
					
				}
			    if(!buckRst.isEmpty()) {
			        keyValueMap.put(buck.getKey(),buckRst);
			    }
			}
	        if(!keyValueMap.isEmpty()) {
	        	keyValueList.add(keyValueMap);
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
		return "timestamp";
	}


	private String[] getMetricFieldByMetric() {
		return new String[]{"flow_size","speed_size","req_count","zeroSpeedReqNum"};
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
	
	private List<String> getIndicesByMetric(){
		if(metric.endsWith("one")) {
			return getOnlineIndices();
		} else {
			return getOfflineIndices();
		}
	}
	
	public List<Map<String, Object>> getResult() {
		return result;
	}

	/*public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}*/

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setHitType(int hitType) {
		this.hitType = hitType;
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
		SOURCE_DOMIMAIN_ONE("source.domain.one"), SOURCE_DOMAIN_FIVE("source.domain.five"), SOURCE_DOMAIN_HOUR("source.domain.hour");/**/
	
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
