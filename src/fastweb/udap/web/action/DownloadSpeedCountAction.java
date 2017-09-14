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
import org.elasticsearch.search.aggregations.Aggregation;
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
@Action(value = "downloadspeedcount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
		   @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class DownloadSpeedCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;
	
	/* metric set to default value: SPEED_TOTAL_ONE */
	private String metric = MetricConst.SPEED_TOTAL_ONE.getMetricStr();
	
	private static int speedIntvNum = 10;
	/* 域名*/
	private String domain;
	/* 查询开始时间戳*/
	private long begin;
	/* 查询结束时间戳*/
	private long end;
	
	private String message;
	/*省份*/
	private String prv = null;
	/* userid */
    private String userid = null;
    
	

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
	
	@Override
	public String execute() throws Exception {
		if(!StringUtil.isEmpty(message)) {
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
				
				if(metric.startsWith("speed.prv.count") && null != prv && !prv.equalsIgnoreCase("all")){
					allQuery = allQuery.must(QueryBuilders.termQuery("prv",this.prv));
				}
				
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
				AbstractAggregationBuilder  builder = null;
				if(metric.startsWith("speed.prv.count")||metric.startsWith("speed.avg.domain")){
					builder = AggregationBuilders.terms(aggFieldName).field(aggFieldName).valueType(Terms.ValueType.STRING).size(Integer.MAX_VALUE).
										subAggregation(metricFieldAgg).subAggregation(AggregationBuilders.sum("req_count").field("req_count"))
										.subAggregation(AggregationBuilders.sum("zeroSpeedReqNum").field("zeroSpeedReqNum"));
					searchBuilder = searchBuilder.addAggregation(builder);
				} else if( metric.startsWith("speed.interval.count")) {
					for(int i=0;i < speedIntvNum;i++){
						searchBuilder = searchBuilder.addAggregation(AggregationBuilders.sum(String.valueOf(i)).field("speed"+i));
					}
				} else {
					builder = AggregationBuilders.terms(aggFieldName).field(aggFieldName).size(Integer.MAX_VALUE).subAggregation(metricFieldAgg);
					searchBuilder = searchBuilder.addAggregation(builder);
				}
				SearchResponse searchResponse = searchBuilder.setFrom(0).setSize(0)
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
	
	private void generateFinalResult(SearchResponse searchResponse, int type) {
		if(0 == type) {
			String  aggFieldName = getAggFieldByMetric();
			if(aggFieldName.equalsIgnoreCase("interval")) {
				List<Aggregation> aggList = searchResponse.getAggregations().asList();
				Map<String,Object> aggMap = new TreeMap<String,Object>();
				for(Aggregation agg :aggList){
					if(agg instanceof Sum) {
						Sum sumAgg = (Sum) agg;
						aggMap.put(sumAgg.getName(), ((Double)sumAgg.getValue()).longValue());
					} else {
						setMessage("Aggregation "+ agg.getName()+" can't transform to Sum ");
						return;
					}
				}
				if(!aggMap.isEmpty()) {
					result.add(aggMap);
				}
			} else {
		        Terms aggFieldTerm = searchResponse.getAggregations().get(aggFieldName);
		        List<Bucket> bucketList = aggFieldTerm.getBuckets();
		        Map<String,Object> aggMap = new HashMap<String,Object>();
		        List<Object> keyValueList = new ArrayList<Object>();
		        aggMap.put(aggFieldName, keyValueList);
		        Map<String,Object> keyValueTurple = new TreeMap<String,Object>();
		        for(Bucket buck:bucketList) {
					String metricFieldName = getMetricFieldByMetric();
					Sum sumAgg = buck.getAggregations().get(metricFieldName);
					
					if(metric.startsWith("speed.prv.count")||metric.startsWith("speed.avg.domain")) /* avgspeed = speedsize/req_count */
					{
						Long speedSize = ((Double)sumAgg.getValue()).longValue();
						Sum sumAgg2 = buck.getAggregations().get("req_count");
						Long reqCount =  ((Double)sumAgg2.getValue()).longValue();
						Sum sumAgg3 = buck.getAggregations().get("zeroSpeedReqNum");
						Long zeroSpeedReqNum = ((Double)sumAgg3.getValue()).longValue();
						if( reqCount.equals(zeroSpeedReqNum)) {
							if( 0 != speedSize ) {
								keyValueTurple.put(buck.getKey(),Integer.MAX_VALUE);
							}
						} else {
							Long avg_speed = speedSize/(reqCount - zeroSpeedReqNum);
							keyValueTurple.put(buck.getKey(),avg_speed);
						}
					} else {
						keyValueTurple.put(buck.getKey(),((Double)sumAgg.getValue()).longValue());
					}
				}
		        if(!keyValueTurple.isEmpty()) {
		        	keyValueList.add(keyValueTurple);
		        }
				result.add(aggMap);
			}
		} else {
		    Iterator<SearchHit> it = searchResponse.getHits().iterator();
			while (it.hasNext()) {
				SearchHit sh = it.next();
				result.add(sh.getSource());
			}
		}
	}
	
	private List<String> getOfflineIndices()
	{
		String [] metricParts = metric.split("\\.");
		String indexPrefix = null;
		List<String> indices = new ArrayList<String>();
		if(metric.startsWith("speed.interval.count")) {
			indices.add("cdnlog.speed."+metricParts[metricParts.length-1]);
			return indices;
		} else {
			indexPrefix = "cdnlog.flow.size.count";
		}
		List<String> indicesSuffix = Time2Type.time2Indices(begin, end);
		
	
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
		if(metric.startsWith("speed.interval.count")) {
			indices.add("cdnlog.speed"); /*速度区间放在cdnlog.speed index中*/
		 
		} else {
			indices.add("cdnlog.flow.size.count");
		}
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
	
	/*不同的metric会统计不同的列*/
	private String getMetricFieldByMetric() {
		if(metric.startsWith("speed.total")||metric.startsWith("speed.avg.domain")||metric.startsWith("speed.prv.count")) {
			return "speed_size";
		}  else if(metric.startsWith("speed.interval.count")){
			return "req_count";
		} else if(metric.startsWith("speed.avg.machine")) {
			return "speed_size";
		} else {
			return "speed_size";
		}
	}

	/*不同的metric会使用不同的列名来分组*/
	private String getAggFieldByMetric() {
		if(metric.startsWith("speed.total")||metric.startsWith("speed.avg.domain")) {
			return "timestamp";
		} else if(metric.startsWith("speed.prv.count")) {
			if(null == prv){
				return "prv";
			} else {
				return "city";
			}
		} else if(metric.startsWith("speed.interval.count")){
			return "interval";
		} else if(metric.startsWith("speed.avg.machine")) {
			return "machine_id";
		} else {
			return "prv";
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
	
	public List<Map<String,Object>> getResult()
	{
		return result;
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
	
	/* 所有支持的metric，会在valid函数中对metric参数做检验，不在该列表中会报错*/
	public enum MetricConst {
		SPEED_TOTAL_ONE("speed.total.one"),SPEED_TOTAL_FIVE("speed.total.five"),SPEED_TOTAL_HOUR("speed.total.hour"),/*下载速度按时间统计*/
		SPEED_AVG_DOMAIN_ONE("speed.avg.domain.one"),SPEED_AVG_DOMAIN_FIVE("speed.avg.domain.five"),SPEED_AVG_DOMAIN_HOUR("speed.avg.domain.hour"),/*平均速度*/
		SPEED_PRV_COUNT_ONE("speed.prv.count.one"),SPEED_PRV_COUNT_FIVE("speed.prv.count.five"),SPEED_PRV_COUNT_HOUR("speed.prv.count.hour"),/*地区平均速度排名*/
		SPEED_INTERVAL_COUNT_ONE("speed.interval.count.one"),SPEED_INTERVAL_COUNT_FIVE("speed.interval.count.five"),SPEED_INTERVAL_COUNT_HOUR("speed.interval.count.hour"),/*速度区间统计*/
		SPEED_MACHINE_COUNT_ONE("speed.machine.count.one"),/*实时按平均速度取前20域名*/
		SPEED_AVG_MACHINE_FIVE("speed.avg.machine.five"),SPEED_AVG_MACHINE_HOUR("speed.avg.machine.hour");/*全量机器视图*/
		
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

