package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.HashMap;
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
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
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
@Action(value = "bossstatuscode", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
		   @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class BossStatusCodeAction extends ActionSupport {
	private static final long serialVersionUID = 1L;
	
	static private String indexPrefix = "cdnlog.boss.status.code";

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
				AbstractAggregationBuilder count = AggregationBuilders.sum("reqCount").field("reqCount");
				//status分类排序
				AbstractAggregationBuilder statusBuilder = AggregationBuilders.terms("statusCode").field("statusCode").subAggregation(count).size(Integer.MAX_VALUE);
				//prv分类排序
				//条件Prv
				AbstractAggregationBuilder subbuilder=null;
				TermsBuilder platFormAgg = AggregationBuilders.terms("platform").field("platform");
				
				//根据统计所有platform
				if(metric.equals("boss.status.code.hour") ){
					subbuilder =platFormAgg.subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					
				}else if(metric.equals("boss.status.code.isp.hour") ){
					//根据platform统计下面所有isp
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
					
				}else if(metric.equals("boss.status.code.prv.hour")){
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);
					
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
					
				}else if(metric.equals("boss.status.code.popid.hour")){
					//根据platform、isp、prv统计下面所有popId
					AbstractAggregationBuilder popIdBuilder = AggregationBuilders.terms("popId").field("popId").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(popIdBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);
					
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
				}else if(metric.equals("boss.status.code.machine.hour")){
					//根据platform、isp、prv、popId统计下面所有machine
					AbstractAggregationBuilder machineBuilder = AggregationBuilders.terms("machineName").field("machineName").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder popIdBuilder = AggregationBuilders.terms("popId").field("popId").subAggregation(machineBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(popIdBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);
					
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
				}else if(metric.equals("boss.status.code.detail.hour")){
					AbstractAggregationBuilder machineBuilder = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					subbuilder = platFormAgg.subAggregation(machineBuilder).size(Integer.MAX_VALUE);
				}
				
				//最后platform汇总
				SearchResponse searchResponse = client.prepareSearch(existsIndices).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(subbuilder).execute().actionGet();
				//result.add(EsSearchUtil.getJsonResult(searchResponse, EsSearchUtil.SearchResultType.AGGREGATIONS));
				
				TreeMap TreeData =  (TreeMap) EsSearchUtil.getJsonResult(searchResponse, EsSearchUtil.SearchResultType.AGGREGATIONS);
				HashMap t1  = new HashMap();
				Map<String,Object> json4 = new TreeMap<String,Object>();
				Map<String,Object> esResult = new TreeMap<String,Object>();
				t1 = (HashMap) TreeData.get("platform");
				//t1 = (HashMap) TreeData.get(platform);
				
				if (!metric.equals("boss.status.code.hour")) {
					t1 = (HashMap) t1.get(platform);
				}
				
				if (metric.equals("boss.status.code.prv.hour")) {
					t1 = (HashMap) t1.get(isp);
				}
				
				if (metric.equals("boss.status.code.popid.hour")) {
					//t1 = (HashMap) t1.get(platform);
					t1 = (HashMap) t1.get(isp);
					t1 = (HashMap) t1.get(prv);
				}
				
				if (metric.equals("boss.status.code.machine.hour")) {
					t1 = (HashMap) t1.get(isp);
					t1 = (HashMap) t1.get(prv);
					t1 = (HashMap) t1.get(popId);
				}
				if (metric.equals("boss.status.code.hour")) {
					esResult.put("platform", t1);
					result.add(esResult);
				}else{
				 json4.put(platform, t1);//加上平台名称
				 esResult.put("platform", json4);
				 result.add(esResult);
				}
				
				
				
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
	@Override
	public void validate() {
		super.validate();
		 
		if(begin > end)
		{
			setMessage("开始时间戳大于结束时间戳");
		}
		if(!MetricConst.getAllMetricStr().contains(metric)){
			setMessage("metric参数值不在给定列表中");
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
		BOSS_STATUS_CODE_ONE_HOUR("boss.status.code.hour"),
		BOSS_STATUS_CODE_PRV_HOUR("boss.status.code.prv.hour"),
		BOSS_STATUS_CODE_POPID_HOUR("boss.status.code.popid.hour"),
		BOSS_STATUS_CODE_MACHINE_HOUR("boss.status.code.machine.hour"),
		BOSS_STATUS_CODE_ISP_HOUR("boss.status.code.isp.hour"),
		BOSS_STATUS_CODE_DETAIL_HOUR("boss.status.code.detail.hour");

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

