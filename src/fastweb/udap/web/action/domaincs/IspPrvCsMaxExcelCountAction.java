package fastweb.udap.web.action.domaincs;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import java.util.List;
import java.util.Map.Entry;
import com.csvreader.CsvWriter; 


import org.apache.commons.lang.xwork.StringUtils;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;

import com.opensymphony.xwork2.ActionSupport;


import fastweb.udap.util.DatetimeUtil;
import fastweb.udap.util.ESUtil;
import fastweb.udap.util.EsSearchUtil;
import fastweb.udap.web.EsClientFactory;


/**
 * 
 * @author blue
 * 
 *查询宽带指定时间段的峰值、当天一天的宽带峰值
 *结果按ISP、PRV分类汇总排序
 */

@Namespace("/")
@Action(value = "ispprvcsmaxexcelcount", results = {
		@Result(name = ActionSupport.SUCCESS, type = "json", params = {
				"excludeProperties", "message,ispHashMap,prvHashMap" }),
		@Result(name = ActionSupport.ERROR, type = "json", params = {
				"includeProperties", "message" }) })
public class IspPrvCsMaxExcelCountAction extends ActionSupport {
	/**
	 * 
	 */
	public static final String INDEX = "cdnlog.flowsize.ispprv.count.five";
	private static final Long DAY =  86400L;
	Client client = EsClientFactory.createClient();
 
	private String domain;
	private String isp;
	private String prv;
	private String status;
	private String userid;
	private String city;
	private String hitType;
	private long begin;
	private long end;
	private long periodBegin;
	private long periodend;
	private String message;
	private List<Object> result = new ArrayList<Object>();

	@SuppressWarnings("unchecked")
	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		try {
			
		 
				/*普通查询方法
				 * 
				 * 
				 */
				BoolQueryBuilder allQuery = null;
				AbstractAggregationBuilder subBuilder = null;
				
				
				String[] existsIndices = ESUtil.getIndicesFromtime(begin,end+DAY,INDEX);

				QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
				
				allQuery = QueryBuilders.boolQuery().must(timestampQuery);

				if (domain != null && !domain.equals("all")) {
					String[] domainArray = domain.split(";");
					allQuery = allQuery.must(QueryBuilders.inQuery("domain",domainArray));
				}
				
				if (userid != null && !userid.equals("all")) {
					String[] useridArray = userid.split(";");
					allQuery = allQuery.must(QueryBuilders.inQuery("userid",useridArray));
				}
				
				if (isp != null && !isp.equals("all") ) {
					String[] ispArray = isp.split(";");
					allQuery = allQuery.must(QueryBuilders.inQuery("isp",ispArray));
				}
				
				if (prv != null && !prv.equals("all")) {
					String[] prvArray = prv.split(";");
					allQuery = allQuery.must(QueryBuilders.inQuery("prv", prvArray));
				 
				}
				
				if (city != null && !city.equals("all")) {
					String[] cityArray = city.split(";");
					allQuery = allQuery.must(QueryBuilders.inQuery("area_cid", cityArray));
				}
				
				if (status != null && !status.equals("all")) {
					String[] statusArray = status.split(";");
					allQuery = allQuery.must(QueryBuilders.inQuery("status", statusArray));
				}
				
				AbstractAggregationBuilder csSum = AggregationBuilders.sum("flow_size").field("flow_size");
				
				subBuilder = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(csSum).size(Integer.MAX_VALUE);

				
				if(status != null){
					 subBuilder = AggregationBuilders.terms("status").field("status").subAggregation(subBuilder).size(Integer.MAX_VALUE);
				}
				
				if(city != null){
					subBuilder = AggregationBuilders.terms("area_cid").field("area_cid").subAggregation(subBuilder).size(Integer.MAX_VALUE);
				}

				
				if(prv != null){
					subBuilder = AggregationBuilders.terms("area_pid").field("area_pid").subAggregation(subBuilder).size(Integer.MAX_VALUE);
				}
				
				if(isp != null){
					subBuilder = AggregationBuilders.terms("isp").field("isp").subAggregation(subBuilder).size(Integer.MAX_VALUE);
				}
			
				
				
				 
				
				/*
				if(!domain.equals("all")){
					subBuilder = AggregationBuilders.terms("domain").field("domain").subAggregation(subBuilder).size(Integer.MAX_VALUE);
				}
				*/
				
				
				SearchResponse response = client.prepareSearch(existsIndices).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(subBuilder).execute().actionGet();
				
				 
				  
				
				//当天最高峰值
				printResult(response,"max");
				System.out.println("==============================================");
			
				
				client.close();
				
				
		
			
			
			/*
			 * 多线程查询
			 * 
			 */	
			/*ExecutorService threadPool  = Executors.newCachedThreadPool();
			CompletionService<HashMap> cs = new ExecutorCompletionService<HashMap>(threadPool);
			
			String[] domainArray = domain.split(";");
			for (String domain : domainArray) {
				Future<HashMap> r1 = cs.submit(new DomainCsQuery(domain, isp,  prv, begin, end));
				result.add(r1.get());
			};
			threadPool.shutdown();*/
			
		 
		} catch (Exception e) {
			e.printStackTrace();
			this.message = "请求错误";
			return ActionSupport.ERROR;
		}
		return ActionSupport.SUCCESS;
	}
	
	
	
	public void printResult(SearchResponse response,String type) throws IOException{
		
		HashMap<String, String> prvMap = getPrvHashMap();
		HashMap<String, String> ispMap = getIspHashMap();
		//String csvMaxFilePath = "/csv/"+type+"-"+domain+"-"+begin+"-"+end+".csv"; 
		//CsvWriter wr =new CsvWriter(csvMaxFilePath,',',Charset.forName("SJIS")); 
	
		TreeMap TreeData =  (TreeMap) EsSearchUtil.getJsonResult(response, EsSearchUtil.SearchResultType.AGGREGATIONS);
		HashMap t1  = new HashMap();
		
		String prv;
		String isp;
		t1 = (HashMap) TreeData.get("isp");
		Iterator<Map.Entry> it = t1.entrySet().iterator();
		while (it.hasNext()) {
			
			 Map.Entry entry = it.next();
			 isp = entry.getKey().toString();
			 HashMap innerData = (HashMap) entry.getValue();
			 Iterator<Map.Entry> innerData2 = innerData.entrySet().iterator();
	
					 while (innerData2.hasNext()) {
						 
						 Map.Entry entry2 = innerData2.next();
						 prv = entry2.getKey().toString();
						 
						 HashMap innerData3 = (HashMap) entry2.getValue();
						 List list= new ArrayList();
						 list.addAll(innerData3.entrySet()); 
						 ValueComparator vc=new ValueComparator(); 
						 Collections.sort(list,vc); 
						 
						 //System.out.println("innerData3="+innerData3);
						 
						//求时间段内的平均峰值
						 if(type.equals("avg")){
							Double avgNum = 0D;
							 Set<String> keys = innerData3.keySet();
							 for (String key : keys) {
								avgNum += Double.parseDouble(innerData3.get(key).toString());
							 }
							 
							 avgNum = avgNum/innerData3.size();
			
							 if(list.size()>0){
								 //System.out.println(list);
								 String[] arr = list.get(0).toString().split("=");
								 
								 String ispTemp = ispMap.get(isp);
								 if(ispTemp != null){
									 if(ispTemp.equals("电信") || ispTemp.equals("移动") || ispTemp.equals("联通")){
										 System.out.println(ispMap.get(isp)+"\t"+prvMap.get(prv)+"\t"+DatetimeUtil.timestampToDateStr(Long.parseLong(arr[0]), "yyyy-MM-dd HH:mm")+"\t"+String.format("%.2f", avgNum));
										 //String[] contents ={ispMap.get(isp),prvMap.get(prv),DatetimeUtil.timestampToDateStr(Long.parseLong(arr[0]), "yyyy-MM-dd HH:mm"),String.format("%.2f", avgNum)};
										 //wr.writeRecord(contents);
									 	}
									 }
								}
						 }
						 
						 
						//求时间段内最高峰值
						 if(type.equals("max")){
							
							 
							 if(list.size()>0){
								 //System.out.println(list);
								 String[] arr = list.get(0).toString().split("=");
								 
								 String ispTemp = ispMap.get(isp);
								 if(ispTemp != null){
									 //if(ispTemp.equals("电信") || ispTemp.equals("移动") || ispTemp.equals("联通")){
										 System.out.println(ispMap.get(isp)+"\t"+prvMap.get(prv)+"\t"+DatetimeUtil.timestampToDateStr(Long.parseLong(arr[0]), "yyyy-MM-dd HH:mm")+"\t"+arr[1]);
										 //String[] contents ={ispMap.get(isp),prvMap.get(prv),DatetimeUtil.timestampToDateStr(Long.parseLong(arr[0]), "yyyy-MM-dd HH:mm"),arr[1]};
										 //wr.writeRecord(contents);
									 //}
									 }
								}
						 }
	
					 }
	
			}
		
		//wr.close(); 
	}
	
	private static class ValueComparator implements Comparator<Map.Entry<String,Long>>  
    {

		@Override
		public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
			// TODO Auto-generated method stub
			return  o2.getValue().compareTo (o1.getValue());
		}  
 
    } 
	
	
	

	@Override
	public void validate() {
		super.validate();
		if (begin > end) {
			setMessage("开始时间戳大于结束时间戳");
		}
		if(end - begin > 31*24*60*1000) {
			setMessage("5分钟和小时数据支持31天查询");
			return;
		}
	}
	
	

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	

	public List<Object> getResult() {
		return result;
	}

	public void setResult(List<Object> result) {
		this.result = result;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setIsp(String isp) {
		this.isp = isp;
	}

	public void setPrv(String prv) {
		this.prv = prv;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public void setHitType(String hitType) {
		this.hitType = hitType;
	}
	
	public void setUserid(String userid) {
		this.userid = userid;
	}
	
	public void setCity(String city) {
		this.city = city;
	}
	
	
	
	public void setPeriodBegin(long periodBegin) {
		this.periodBegin = periodBegin;
	}



	public void setPeriodend(long periodend) {
		this.periodend = periodend;
	}



	public HashMap<String, String> getPrvHashMap(){
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("1000","美国");
		map.put("1009","加勒比海");
		map.put("1100","加拿大");
		map.put("2300","非洲");
		map.put("3100","中东");
		map.put("3300","法国");
		map.put("3400","西班牙");
		map.put("3800","乌克兰");
		map.put("3900","意大利");
		map.put("4400","英国");
		map.put("4900","德国");
		map.put("5200","墨西哥");
		map.put("5400","阿根廷");
		map.put("5500","巴西");
		map.put("5980","加勒比海");
		map.put("6000","马来西亚");
		map.put("6100","太平洋");
		map.put("6200","印度尼西亚");
		map.put("6300","菲律宾");
		map.put("6400","新加坡");
		map.put("6600","泰国");
		map.put("7000","俄罗斯");
		map.put("8100","日本");
		map.put("8200","韩国");
		map.put("8400","越南");
		map.put("8520","香港特区");
		map.put("8530","澳门特区");
		map.put("8609","中国其它");
		map.put("8610","北京");
		map.put("8620","广东省");
		map.put("8621","上海");
		map.put("8622","天津");
		map.put("8623","重庆");
		map.put("8624","辽宁省");
		map.put("8625","江苏省");
		map.put("8627","湖北省");
		map.put("8628","四川省");
		map.put("8629","陕西省");
		map.put("8631","河北省");
		map.put("8634","山西省");
		map.put("8637","河南省");
		map.put("8643","吉林省");
		map.put("8645","黑龙江省");
		map.put("8647","内蒙古自治区");
		map.put("8653","山东省");
		map.put("8655","安徽省");
		map.put("8657","浙江省");
		map.put("8659","福建省");
		map.put("8669","云南省");
		map.put("8670","江西省");
		map.put("8673","湖南省");
		map.put("8677","广西壮族自治区");
		map.put("8685","贵州省");
		map.put("8688","西藏自治区");
		map.put("8689","海南省");
		map.put("8690","新疆维吾尔自治区");
		map.put("8693","甘肃省");
		map.put("8695","宁夏回族自治区");
		map.put("8697","青海省");
		map.put("8860","台湾省");
		map.put("9000","土耳其");
		map.put("9100","印度");
		map.put("9660","沙特阿拉伯");
		return map;
	}
	
	public HashMap<String, String> getIspHashMap(){
		HashMap<String, String> map = new HashMap<String, String>();
		map.put("11","电信");
		map.put("12","联通");
		map.put("13","移动");
		map.put("14","联通");
		map.put("15","铁通");
		map.put("16","广电");
		map.put("17","电信通");
		map.put("18","教育网");
		map.put("19","国内其他");
		map.put("20","其他");
		map.put("21","德国");
		map.put("22","双线");
		map.put("23","长城宽带");
		map.put("24","天威");
		map.put("25","有线通");
		return map;
	
	}
}
