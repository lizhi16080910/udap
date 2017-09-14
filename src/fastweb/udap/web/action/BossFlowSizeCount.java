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
import org.elasticsearch.action.percolate.TransportShardMultiPercolateAction.Response;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.MultiSearchResponse.Item;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.ScriptFilterBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.ESUtil;
import fastweb.udap.util.EsSearchUtil;
import fastweb.udap.util.StringUtil;
import fastweb.udap.util.Time2Type;
import fastweb.udap.web.EsClientFactory;
import fastweb.udap.web.action.DownloadSpeedCountAction.MetricConst;

/**
 * 后缀流量
 */
@Namespace("/")
@Action(value = "bossflowsizecount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
		   @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class BossFlowSizeCount extends ActionSupport {
	private static final long serialVersionUID = 1L;
	
	static private String indexPrefix = "cdnlog.boss.flowsize";

	/*开始时间戳*/
	private long begin;
	/*结束时间戳*/
	private long end;
	/*错误信息*/
	private String message;
	
	/* 省份*/
	private String prv = null;
	
	private String isp = null;
	
	/* */
	private String platform = null;
	
	private String machineName = null;
	
	private String popId = null;
	
	private String statusCode = null;
	
	private String metric = null;
	

	private List<Object> result = new ArrayList<Object>();
	
	@Override
	public String execute() throws Exception {
		
		if(!StringUtil.isEmpty(message)) {
			return ActionSupport.ERROR;
		}
		try {
			
			Client client = EsClientFactory.createClient();
			
			
			String[] existsIndices = ESUtil.getExistIndices(getIndicesByMetric()).toArray( new String[0]);
			
			try {
				QueryBuilder timeQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timeQuery);
				
			    //ScriptFilterBuilder filterBuilder = FilterBuilders.scriptFilter("doc['isp'].value ==  doc['machineIspCode'].value && doc['prv'].value ==  doc['machinePrvCode'].value  ");
			
				
				//条件
			  //条件
				if(platform  != null){
					allQuery = allQuery.must(QueryBuilders.termQuery("platform",this.platform));
				}
				if(isp  != null){
					allQuery = allQuery.must(QueryBuilders.termQuery("machineIsp",this.isp));
				}
				if(prv  != null){
					allQuery = allQuery.must(QueryBuilders.termQuery("machinePrv",this.prv));
				}
				if(popId  != null){
					allQuery = allQuery.must(QueryBuilders.termQuery("popId",this.popId));
				}
				if(machineName  != null){
					allQuery = allQuery.must(QueryBuilders.termQuery("machineName",this.machineName));
				}
				if(statusCode  != null){
					allQuery = allQuery.must(QueryBuilders.termQuery("statusCode",this.statusCode));
				}
				 
				
				//汇总字段
				
				AbstractAggregationBuilder count = AggregationBuilders.sum("flowSize").field("flowSize");
				//条件Prv
				AbstractAggregationBuilder subbuilder=null;
			
				//status分类排序
				AbstractAggregationBuilder statusBuilder = AggregationBuilders.terms("isLocalFlow").field("isLocalFlow").subAggregation(count).size(Integer.MAX_VALUE);
				
				 
				
				TermsBuilder platFormAgg = AggregationBuilders.terms("platform").field("platform");
				
				//根据统计所有platform
				if(metric.equals("boss.flow.size.hour") ){
					subbuilder =platFormAgg.subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					
				}else if(metric.equals("boss.flow.size.isp.hour") ){
					//根据platform统计下面所有isp
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
					
				}else if(metric.equals("boss.flow.size.prv.hour")){
					//根据platform、isp统计下面所有prv
					
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);
					
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
					
				}else if(metric.equals("boss.flow.size.popid.hour")){
					//根据platform、isp、prv统计下面所有popId
					AbstractAggregationBuilder popIdBuilder = AggregationBuilders.terms("popId").field("popId").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(popIdBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);
					
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
				
				}else if(metric.equals("boss.flow.size.machine.hour")){
					//根据platform、isp、prv、popId统计下面所有machine
					AbstractAggregationBuilder machineBuilder = AggregationBuilders.terms("machineName").field("machineName").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder popIdBuilder = AggregationBuilders.terms("popId").field("popId").subAggregation(machineBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(popIdBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);
					
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
				}else  if(metric.equals("boss.flow.size.platform.hour")){
					AbstractAggregationBuilder popIdFormAgg = AggregationBuilders.terms("popId").field("popId").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder prvIdFormAgg = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(popIdFormAgg).size(Integer.MAX_VALUE);
					subbuilder  = AggregationBuilders.terms("platform").field("platform").subAggregation(prvIdFormAgg).size(Integer.MAX_VALUE);
					
				}
				//最后platform汇总 addScriptField("flag", "doc['isp'].value ==  doc['machineIspCode'].value && doc['prv'].value ==  doc['machinePrvCode'].value ? 1:0")
				//最后platform汇总
				
				SearchResponse response = client.prepareSearch(existsIndices).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(subbuilder).execute().actionGet();
				//SearchResponse searchResponse = client.prepareSearch(existsIndices).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(subbuilder).execute().actionGet();
				//格式化结果

				
				if(metric.equals("boss.flow.size.platform.hour")){
					 getPlatformHour(response);
				}else{
					TreeMap TreeData =  (TreeMap) EsSearchUtil.getJsonResult(response, EsSearchUtil.SearchResultType.AGGREGATIONS);
					HashMap t1  = new HashMap();
					Map<String,Object> json4 = new TreeMap<String,Object>();
					Map<String,Object> esResult = new TreeMap<String,Object>();
					t1 = (HashMap) TreeData.get("platform");
					
					
					if (!metric.equals("boss.flow.size.hour")) {
						t1 = (HashMap) t1.get(platform);
					}
					
					/*if (metric.equals("boss.flow.size.isp.hour")) {
						t1 = (HashMap) t1.get(isp);
					}*/
					
					if (metric.equals("boss.flow.size.prv.hour")) {
						t1 = (HashMap) t1.get(isp);
					}
					
					if (metric.equals("boss.flow.size.popid.hour")) {
						t1 = (HashMap) t1.get(isp);
						t1 = (HashMap) t1.get(prv);
					}
					
					if (metric.equals("boss.flow.size.machine.hour")) {
						t1 = (HashMap) t1.get(isp);
						t1 = (HashMap) t1.get(prv);
						t1 = (HashMap) t1.get(popId);
					}
					if (metric.equals("boss.flow.size.hour")) {
						esResult.put("platform", t1);
						result.add(esResult);
					}else{
					 json4.put(platform, t1);//加上平台名称
					 esResult.put("platform", json4);
					 result.add(esResult);
					}
					 
				}
				
				/*SearchResponse srb1 = client.prepareSearch(existsIndices)
				.setQuery(allQuery.must(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(),filterBuilder)))
				.setFrom(0).setSize(0).addAggregation(subbuilder).execute().actionGet();
				
				result.add(EsSearchUtil.getJsonResult(srb1, EsSearchUtil.SearchResultType.AGGREGATIONS));*/
				
				
				
				//批量查询
				/*SearchRequestBuilder searchBuilder = client.prepareSearch(existsIndices)
				.setQuery(allQuery.must( QueryBuilders.filteredQuery(QueryBuilders.boolQuery(),filterBuilder)));
				
				SearchRequestBuilder searchBuilder2 = client.prepareSearch(existsIndices)
				.setQuery(allQuery);
				
				
				MultiSearchRequestBuilder multiRequestBuilder = new MultiSearchRequestBuilder(client);
				multiRequestBuilder.add(searchBuilder.setFrom(0).setSize(0).addAggregation(subbuilder));
				multiRequestBuilder.add(searchBuilder2.setFrom(0).setSize(0).addAggregation(subbuilder));
				
				MultiSearchResponse responses = multiRequestBuilder.execute().actionGet();
				
				for(Item it:responses.getResponses()) {
					SearchResponse response = it.getResponse();
					result.add(EsSearchUtil.getJsonResult(response, EsSearchUtil.SearchResultType.AGGREGATIONS));
				}*/
				 
				
				
				/*
				MultiSearchResponse sr = client.prepareMultiSearch()
				        .add(srb1)
				        .execute().actionGet();

				for (MultiSearchResponse.Item item : sr.getResponses()) {
				    SearchResponse response = item.getResponse();
				    result.add(EsSearchUtil.getJsonResult(response, EsSearchUtil.SearchResultType.AGGREGATIONS));
				}*/
				
				
			} finally {
				client.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			
		} finally {
		}
		return ActionSupport.SUCCESS;
	}




	public void setEnd(long end) {
		this.end = end;
	}
	public List<Object> getResult()
	{
		return result;
	}
	public String getMessage() {
		return message;
	}
	public void setBegin(long begin) {
		this.begin = begin;
	}
	public void setpopId(String popId) {
		this.popId = popId;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public void setPrv(String prv) {
		this.prv = prv;
	}
	public void setIsp(String isp) {
		this.isp = isp;
	}
	public void setPlatform(String platform) {
		this.platform = platform;
	}
	public void setMachineName(String machineName) {
		this.machineName = machineName;
	}
	public void setPopId(String popId) {
		this.popId = popId;
	}
	public void setStatusCode(String statusCode) {
		this.statusCode = statusCode;
	}
	public void setMetric(String metric) {
		this.metric = metric;
	}
	

	/* check the validity of parameters pass in */
	/* 参数校验*/
	@Override
	public void validate() {
		super.validate();
		 
		
		if(begin > end)
		{
			setMessage("开始时间戳大于结束时间戳");
			return;
		} else {
			if(metric.endsWith("one") && end - begin > 3*60*1000) {
				setMessage("分钟数据支持三小时查询");
				return;
			} else if(end - begin > 45*24*60*1000) {
				setMessage("5分钟和小时数据支持45天查询");
				return;
			}
		}

		if(!MetricConst.getAllMetricStr().contains(metric)){
			setMessage("metric参数值不在给定列表中");
			return;
		}
	}
	
	
	/* 根据用户metric参数选择需要查询的Es index*/
	private List<String> getIndicesByMetric()
	{
		if(metric.endsWith("one")){
			return getOnlineIndices();
		} else {
			return getOfflineIndices();
		}
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
	
	
	
	/* 所有支持的metric，会在valid函数中对metric参数做检验，不在该列表中会报错*/
	public enum MetricConst {
		BOSS_FLOW_SIZE_HOUR("boss.flow.size.hour"),
		BOSS_FLOW_SIZE_PRV_HOUR("boss.flow.size.prv.hour"),
		BOSS_FLOW_SIZE_POPID_HOUR("boss.flow.size.popid.hour"),
		BOSS_FLOW_SIZE_MACHINE_HOUR("boss.flow.size.machine.hour"),
		BOSS_FLOW_SIZE_PLATFORM_HOUR("boss.flow.size.platform.hour"),
		BOSS_FLOW_SIZE_ISP_HOUR("boss.flow.size.isp.hour");

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
	

	 
	
	private List<Object> getPlatformHour(SearchResponse response) {
		//格式化结果
		TreeMap TreeData =  (TreeMap) EsSearchUtil.getJsonResult(response,EsSearchUtil.SearchResultType.AGGREGATIONS);
		String platform_t ;
		String prv_t;
		String popId_t;
		String flowSize_t;
		String isLocal_t;
		Map<String,Object> json1 = null;
		 
		
		HashMap HashData = (HashMap) TreeData.get("platform");
		Iterator<Map.Entry> it = HashData.entrySet().iterator();
		
		//第一层循环，取platform
		while (it.hasNext()) {
			 Map.Entry entry = it.next();
			 platform_t = entry.getKey().toString();
			 
			 HashMap innerData = (HashMap) entry.getValue();
			 Iterator<Map.Entry> innerData2 = innerData.entrySet().iterator();
			 TreeMap<String,Object> json2=new TreeMap<String,Object>(); 
			 
			//第二层循环，取prv
			 while (innerData2.hasNext()) {
				 json1 = new TreeMap<String,Object>();
				 json1.put("platform", platform_t);
				  Map.Entry entry2 = innerData2.next();
				  prv_t = entry2.getKey().toString();
				  
				  //System.out.println("platform_t="+platform_t);
				  //System.out.println("prv_t="+prv_t);
				  
				  json1.put("prv", prv_t);
				   
				   HashMap innerData3 =  (HashMap) entry2.getValue();
				   Iterator<Map.Entry> innerData4 = innerData3.entrySet().iterator();
				   TreeMap<String,Object> json=new TreeMap<String,Object>(); 
				   
				 //第三层循环，取popId
				   while (innerData4.hasNext()) {
					   Map.Entry entry5 = innerData4.next();
					   popId_t = entry5.getKey().toString();
					   json1.put("popId", popId_t);
					   //System.out.println("popId_t="+popId_t);
					   HashMap innerData5 = (HashMap) entry5.getValue();
					   Iterator<Map.Entry> innerData6 =  innerData5.entrySet().iterator();
					   
					 //第四层循环，取flowsize
					   Long flowSizeCount = 0L;
					   int flag = 0;
					  
					   while (innerData6.hasNext()) {
						   Map.Entry entry6 = innerData6.next(); 
						   isLocal_t = entry6.getKey().toString();
						   flowSize_t = entry6.getValue().toString();
						   flowSizeCount = Long.parseLong(flowSize_t)+flowSizeCount;
						   //json6.put(isLocal_t, flowSize_t); 
						   if(isLocal_t.equals("0")) json1.put("flowSize", flowSize_t);
						   flag ++;
						   //System.out.println("isLocal_t="+isLocal_t);
						  // System.out.println("flowSize_t="+flowSize_t);
					   }
					   //判断只有本地覆盖，没有偏差宽带的情况，默认补0
					   //if(flag<2) json1.put("flowSize", 0);
					   
					   json1.put("totalSize", flowSizeCount);
				   }
				   if(!json1.isEmpty()) result.add(json1);
			 }
			 
		}
		return result;
	}
	
	
}

