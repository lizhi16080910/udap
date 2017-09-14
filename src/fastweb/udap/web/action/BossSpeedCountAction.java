package fastweb.udap.web.action;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.json.JSONUtil;
import org.eclipse.jetty.util.ajax.JSON;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.json.JSONArray;
import org.json.JSONObject;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.ESUtil;
import fastweb.udap.util.EsSearchUtil;
import fastweb.udap.util.StringUtil;
import fastweb.udap.util.Time2Type;
import fastweb.udap.web.EsClientFactory;

/**
 * 后缀流量
 */
@Namespace("/")
@Action(value = "bossspeedcount", results = {@Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties", "message" }),@Result(name = ActionSupport.ERROR, type = "json", params = {"includeProperties", "message" }) })
public class BossSpeedCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	static private String indexPrefix = "cdnlog.boss.speed.order";

	/* 开始时间戳 */
	private long begin;
	/* 结束时间戳 */
	private long end;
	/* 错误信息 */
	private String message;

	/* 省份 */
	private String prv = null;

	private String isp = null;

	/* */
	private String platform = null;

	private String machineName = null;

	private String popId = null; 

	private String statusCode = null;
	
	private String domain = null;

	private String metric = null;
	
	private int sortNum = 20;

	private List<Object> result = new ArrayList<Object>();
	
	private List<Object> resultTemp = new ArrayList<Object>();

	@Override
	public String execute() throws Exception {

		if (!StringUtil.isEmpty(message)) {
			return ActionSupport.ERROR;
		}
		try {

			Client client = EsClientFactory.createClient();

			String[] existsIndices = ESUtil.getExistIndices(
					getIndicesByMetric()).toArray(new String[0]);

			try {
				QueryBuilder timeQuery = QueryBuilders.rangeQuery("timestamp")
						.from(begin).to(end);
				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(
						timeQuery);

				// 条件
				if (platform != null) {
					allQuery = allQuery.must(QueryBuilders.termQuery("platform", this.platform));
				}
				if (isp != null) {
					allQuery = allQuery.must(QueryBuilders.termQuery("machineIsp", this.isp));
				}
				if (prv != null) {
					allQuery = allQuery.must(QueryBuilders.termQuery("machinePrv", this.prv));
				}
				if (popId != null) {
					allQuery = allQuery.must(QueryBuilders.termQuery("popId",this.popId));
				}
				if (machineName != null) {
					allQuery = allQuery.must(QueryBuilders.termQuery("machineName", this.machineName));
				}
				if (statusCode != null) {
					allQuery = allQuery.must(QueryBuilders.termQuery("statusCode", this.statusCode));
				}

				// 汇总字段
				AbstractAggregationBuilder count = AggregationBuilders.sum("speed").field("speed");
				// status分类排序

				AbstractAggregationBuilder reqCount = AggregationBuilders.sum("reqCount").field("reqCount");
				 

				AbstractAggregationBuilder statusBuilder = AggregationBuilders.terms("hitType").field("hitType").subAggregation(count).size(Integer.MAX_VALUE).subAggregation(reqCount);
				// prv分类排序
				// 条件Prv
				AbstractAggregationBuilder subbuilder = null;
				TermsBuilder platFormAgg = AggregationBuilders.terms("platform").field("platform");

				// 根据统计所有platform
				if (metric.equals("boss.speed.hour")) {
					subbuilder = platFormAgg.subAggregation(statusBuilder).size(Integer.MAX_VALUE);

				} else if (metric.equals("boss.speed.isp.hour")) {
					// 根据platform统计下面所有isp
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);

				} else if (metric.equals("boss.speed.prv.hour")) {
					// 根据platform、isp统计下面所有prv
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);

					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);

				} else if (metric.equals("boss.speed.popid.hour")) {
					// 根据platform、isp、prv统计下面所有popId
					AbstractAggregationBuilder popIdBuilder = AggregationBuilders.terms("popId").field("popId").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(popIdBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);

					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);

				} else if (metric.equals("boss.speed.machine.hour")) {
					// 根据platform、isp、prv、popId统计下面所有machine
					AbstractAggregationBuilder machineNameBuilder = AggregationBuilders.terms("machineName").field("machineName").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder popIdBuilder = AggregationBuilders.terms("popId").field("popId").subAggregation(machineNameBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(popIdBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);

					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
				} else if (metric.equals("boss.speed.domain.hour")) {
					 
					AbstractAggregationBuilder domainBuilder = AggregationBuilders.terms("domain").field("domain").subAggregation(statusBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder machineNameBuilder = AggregationBuilders.terms("machineName").field("machineName").subAggregation(domainBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder popIdBuilder = AggregationBuilders.terms("popId").field("popId").subAggregation(machineNameBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder prvBuilder = AggregationBuilders.terms("machinePrv").field("machinePrv").subAggregation(popIdBuilder).size(Integer.MAX_VALUE);
					AbstractAggregationBuilder ispBuilder = AggregationBuilders.terms("machineIsp").field("machineIsp").subAggregation(prvBuilder).size(Integer.MAX_VALUE);
					
					subbuilder = platFormAgg.subAggregation(ispBuilder).size(Integer.MAX_VALUE);
				}

				// 最后platform汇总
				SearchResponse searchResponse = client.prepareSearch(existsIndices).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(subbuilder).execute().actionGet();
				
				 
				 
				// 计算平均速度 请求数之和/速度之和
				TreeMap TreeData =  (TreeMap) EsSearchUtil.getJsonResult(searchResponse,EsSearchUtil.SearchResultType.AGGREGATIONS);
				HashMap HashData  = new HashMap();
				DecimalFormat df = new DecimalFormat("#.##");
				
				/*
				if (metric.equals("boss.speed.hour")) {
					HashData = (HashMap) TreeData.get("platform");
				}else{
					HashData = (HashMap) TreeData.get("platform");
					HashData =   (HashMap) HashData.get(platform);
				}*/
				
				//去除外面的嵌套
				HashMap t1  = new HashMap();
				
				t1 = (HashMap) TreeData.get("platform");
				if (!metric.equals("boss.speed.hour")) {
					t1 = (HashMap) t1.get(platform);
				}
				
				/*if (metric.equals("boss.speed.isp.hour")) {
					t1 = (HashMap) t1.get(isp);
				}*/
				
				if (metric.equals("boss.speed.prv.hour")) {
					t1 = (HashMap) t1.get(isp);
				}
				
				if (metric.equals("boss.speed.popid.hour")) {
					t1 = (HashMap) t1.get(isp);
					t1 = (HashMap) t1.get(prv);
				}
				
				if (metric.equals("boss.speed.machine.hour")) {
					t1 = (HashMap) t1.get(isp);
					t1 = (HashMap) t1.get(prv);
					t1 = (HashMap) t1.get(popId);
				}
				
				if (metric.equals("boss.speed.domain.hour")) {
					t1 = (HashMap) t1.get(isp);
					t1 = (HashMap) t1.get(prv);
					t1 = (HashMap) t1.get(popId);
					t1 = (HashMap) t1.get(machineName);
				}
				
				
				
				
				Iterator<Map.Entry> it = t1.entrySet().iterator();
				
				Map<String,Object> json3 = new TreeMap<String,Object>();
				Map<String,Object> json4 = new TreeMap<String,Object>();
				
				TreeMap<String,Object> json5 = new TreeMap<String,Object>(new Comparator<Object>() {
		               public int compare(Object o1, Object o2) {
		                      //如果有空值，直接返回0
		                      if (o1 == null || o2 == null)
		                          return 0; 
		                     return Double.valueOf((String) o1).compareTo(Double.valueOf((String) o2));
		               }
		      });
				
				//遍历取值
				while (it.hasNext()) {
					 Map.Entry entry = it.next();
					 //System.out.println( entry.getKey());
					 Long sumSpeed = 0L;
					 Long speed = 0L;
					 Long sumreqCount=0L;
					 Double avgSpeed=0.0;
					 
					 HashMap innerData = (HashMap) entry.getValue();
					 
					 Iterator<Map.Entry> innerData2 = innerData.entrySet().iterator();
					 TreeMap<String,Object> json2=new TreeMap<String,Object>(); 
							 
						while (innerData2.hasNext()) {
								   Map.Entry entry2 = innerData2.next();
								   TreeMap innerData3 = (TreeMap) entry2.getValue();
								   Iterator<Map.Entry> innerData4 = innerData3.entrySet().iterator();
								   TreeMap<String,Object> json=new TreeMap<String,Object>(); 
								   while (innerData4.hasNext()) {
									   Map.Entry entry5 = innerData4.next();
									   
									  if(entry5.getKey().equals("reqCount")){
										  String jsonreqCount= entry5.getValue().toString();
										  //System.out.println("jsonreqCount="+jsonreqCount);
										 json.put("reqCount", jsonreqCount);
										 
										  sumreqCount += Long.parseLong(jsonreqCount);
									  }
									  
									  if(entry5.getKey().equals("speed")){
										  String jsonSpeed = entry5.getValue().toString();
										  speed = Long.parseLong(jsonSpeed);
										 // System.out.println("jsonSpeed="+jsonSpeed);
										  json.put("speed", speed);
										  
										  sumSpeed += speed;
									  }
								   }
								  // json2.put((String) entry2.getKey(), speed);//根据不同的hitType，只放入speed
								   json2.put((String) entry2.getKey(), json);//放入reqCount和hitType
								   //System.out.println("entry2.getKey()="+entry2.getKey());
							 }
							 if(sumreqCount > 0){
								 avgSpeed = (double)sumSpeed/(double)sumreqCount;
								 //System.out.println(avgSpeed);
							 }else{
								 avgSpeed = 0.0;
							 }
							
							 json2.put("avgspeed",  df.format(avgSpeed));
							 
							 //System.out.println("avgSpeed="+avgSpeed);
							 
							 json3.put((String) entry.getKey(), json2);//加上平台名称
							
							 //域名排序需求
							if (metric.equals("boss.speed.domain.hour")){
								json2.put("domain", (String) entry.getKey());
								json5.put(df.format(avgSpeed), json2);//加上平台名称
							}
							 
							 
			}
				
				
				
				//域名排序需求
				LinkedHashMap<String,Object>    json6 =null;
				int flag = json5.size();
				 
				if (metric.equals("boss.speed.domain.hour")){
					json6 = new LinkedHashMap();
					for(int i=0;i < sortNum && i < flag ;i++){
						TreeMap entry = (TreeMap) json5.lastEntry().getValue();
					   //取得排序后的domain
						json6.put( (String) entry.get("domain"), entry);
						json5.remove(json5.lastKey());
					}
					
				}
				
				
				Map<String,Object> esResult = new TreeMap<String,Object>();
				
				if(metric.equals("boss.speed.hour")){
					esResult.put("platform", json3);//加上平台名称
				}else if(metric.equals("boss.speed.domain.hour")){
					 json4.put(platform, json6);//加上平台名称
					 esResult.put("platform", json4);
				}else{
					 json4.put(platform, json3);//加上平台名称
					 esResult.put("platform", json4);
				}
				
				
				
			
				
				
				result.add(esResult);
			}finally {
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

	public List<Object> getResult() {
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
	

	public int getSortNum() {
		return sortNum;
	}



	public void setSortNum(int sortNum) {
		this.sortNum = sortNum;
	}



	public void setDomain(String domain) {
		this.domain = domain;
	}

	/* check the validity of parameters pass in */
	@Override
	public void validate() {
		super.validate();

		if (begin > end) {
			setMessage("开始时间戳大于结束时间戳");
		}
		if (!MetricConst.getAllMetricStr().contains(metric)) {
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

	/* 所有支持的metric，会在valid函数中对metric参数做检验，不在该列表中会报错 */
	public enum MetricConst {
		BOSS_STATUS_CODE_HOUR("boss.speed.hour"), 
		BOSS_STATUS_CODE_PRV_HOUR("boss.speed.prv.hour"), 
		BOSS_STATUS_CODE_POPID_HOUR("boss.speed.popid.hour"),
		BOSS_STATUS_CODE_MACHINE_HOUR("boss.speed.machine.hour"), 
		BOSS_STATUS_CODE_ISP_HOUR("boss.speed.isp.hour"),
		BOSS_SPEED_DOMAIN_HOUR("boss.speed.domain.hour"),
		BOSS_SPEED_DOMAIN_ISP_HOUR("boss.speed.domain.isp.hour"),
		BOSS_SPEED_DOMAIN_DETAIL_HOUR("boss.speed.domain.detail.hour");

		private String metricStr;

		private MetricConst(String metricStr) {
			this.metricStr = metricStr;
		}

		private String getMetricStr() {
			return this.metricStr;
		}

		private static Set<String> getAllMetricStr() {
			Set<String> rst = new TreeSet<String>();
			for (MetricConst value : values()) {
				rst.add(value.getMetricStr());
			}
			return rst;
		}
	}
}
