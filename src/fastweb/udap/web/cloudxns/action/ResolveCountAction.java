package fastweb.udap.web.cloudxns.action;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.lang.xwork.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.ServletActionContext;
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
import org.elasticsearch.search.sort.SortOrder;
import com.opensymphony.xwork2.ActionSupport;
import fastweb.udap.util.ESUtil;
import fastweb.udap.util.EsSearchUtil;
import fastweb.udap.web.EsClientFactory;


@Namespace("/")
@Action(value = "resolvecount", results = {
		@Result(name = ActionSupport.SUCCESS, type = "json", params = {
				"excludeProperties", "message" }),
		@Result(name = ActionSupport.ERROR, type = "json", params = {
				"includeProperties", "message" }) })
public class ResolveCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "cdnlog.cloudxns";
	private static final Long DAY =  86400L;

	private String host;
	private String userid;
	private String viewid;
	private String zone;
	private String isp;
	private String region;
	private long begin;
	private long end;
	private String message;
	private List<Object> result = new ArrayList<Object>();

	@Override
	public String execute() throws Exception {
		
		
		
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {


			client = EsClientFactory.createClient();
			
			
				/*普通查询方法
				 * 为了避免数据出现超时，往后多查一天
				 * 
				 */
			BoolQueryBuilder allQuery = null;

			String[] existsIndices = ESUtil.getIndicesFromMonth(begin,end+DAY,INDEX);

			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);

			allQuery = QueryBuilders.boolQuery().must(timestampQuery);
			
			//判断主机  为 all 表示查询所有主机   为@查询@主机 为空查询所有

			if (host != null && !host.equals("all")) {

				if (host.equals("@")) {

					String hostterm = host;
					allQuery = allQuery.must(QueryBuilders.termQuery("host",
							hostterm));
				} else {
					String hostterm = host + ".";
					allQuery = allQuery.must(QueryBuilders.termQuery("host",
							hostterm));
				}

			}
			
			if (userid != null && !userid.equals("all")) {
				String[] useridArray = userid.split(";");
				allQuery = allQuery.must(QueryBuilders.inQuery("userid",useridArray));
			}

			if (zone != null && !zone.equals("all")) {
				String zoneterm = zone+".";
				allQuery = allQuery.must(QueryBuilders.termQuery("zone",zoneterm));
			}
			
			if (viewid != null && !viewid.equals("all")) {
				allQuery = allQuery.must(QueryBuilders.termQuery("viewid",viewid));
			}
			
			if (isp != null && !isp.equals("all")) {
				allQuery = allQuery.must(QueryBuilders.termQuery("isp",isp));
			}
			
			if (region != null && !region.equals("all")) {
				allQuery = allQuery.must(QueryBuilders.termQuery("region",region));
			}
			
			
			//聚合条件


			AbstractAggregationBuilder subBuilder = AggregationBuilders.sum("count").field("count");


			if (isp == null && region == null) {

				subBuilder = AggregationBuilders.terms("timestamp")
						.field("timestamp").subAggregation(subBuilder).size(Integer.MAX_VALUE);
			}

			if (host != null) {
				subBuilder = AggregationBuilders.terms("host").field("host")
						.subAggregation(subBuilder).size(Integer.MAX_VALUE);
			}

			if (userid != null) {
				subBuilder = AggregationBuilders.terms("userid").field("userid")
						.subAggregation(subBuilder).size(Integer.MAX_VALUE);
			}

			if (zone != null) {
				subBuilder = AggregationBuilders.terms("zone").field("zone")
						.subAggregation(subBuilder).size(Integer.MAX_VALUE);
			}
			
			if (viewid != null) {
				subBuilder = AggregationBuilders.terms("viewid").field("viewid")
						.subAggregation(subBuilder).size(Integer.MAX_VALUE);
			}
			
			if (isp != null) {
				subBuilder = AggregationBuilders.terms("isp").field("isp")
						.subAggregation(subBuilder).size(Integer.MAX_VALUE);
			}
			
			if (region != null) {
				subBuilder = AggregationBuilders.terms("region").field("region")
						.subAggregation(subBuilder).size(Integer.MAX_VALUE);
			}
			


			searchRequestBuilder = client.prepareSearch(existsIndices).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(subBuilder).addSort("timestamp", SortOrder.DESC);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());


			SearchResponse response = searchRequestBuilder.execute().actionGet();
			
//			System.out.println(response);

			TreeMap TreeData = (TreeMap) EsSearchUtil.getJsonResult(response,
					EsSearchUtil.SearchResultType.AGGREGATIONS);
			result.add(TreeData);
			
			
			
//			JSONArray jsonarry = new JSONArray();
//			LongTerms useridAgge = response.getAggregations().get("userid");
//			for (Bucket useridbucket : useridAgge.getBuckets()) {
//				Long useridkey = Long.valueOf(useridbucket.getKey());
//				JSONObject json = new JSONObject();
//				json.put("userid", useridkey);
//				JSONArray jsonarry2 = new JSONArray();
//				LongTerms timestampAgge = useridbucket.getAggregations().get(
//						"timestamp");
//				for (Bucket timestampbucket : timestampAgge.getBuckets()) {
//					long timestampkey = Long.valueOf(timestampbucket.getKey());
//					Sum sum = timestampbucket.getAggregations().get("count");
//					long countvalue = ((Double) sum.getValue()).longValue();
//					JSONObject json2 = new JSONObject();
//					json2.put("timestamp", timestampkey);
//					json2.put("count", countvalue);
//					jsonarry2.add(json2);
//					
//				}
//				json.put("data", jsonarry2);
//				jsonarry.add(json);
//			}
			
//			this.result=jsonarry;
			
				


		} catch (Exception e) {
			this.message = "请求失败";
			log.error("Query Fail: " + e);
			return ActionSupport.ERROR;
		} finally {
			if(client != null){
				client.close();
			}
		}
		return ActionSupport.SUCCESS;
	}

	@Override
	public void validate() {
		super.validate();
		if (begin > end) {
			setMessage("开始时间戳大于等于结束时间戳");
		}
		
		if(end - begin >= 5184000)
		{
			setMessage("查询时间间隔不能超过2个月");
		}
		
	}



	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

//	public Map<Long, Map<Long, Long>> getResult() {
//		return result;
//	}
	
	public List<Object> getResult() {
		return result;
	}

//	public void setResult(Map<Long,Long> result) {
//		this.result = result;
//	}
	

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}


	public void setUserid(String userid) {
		this.userid = userid;
	}
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public void setZone(String zone) {
		this.zone = zone;
	}
	
	public void setViewid(String viewid) {
		this.viewid = viewid;
	}
	
	public void setIsp(String isp) {
		this.isp = isp;
	}
	
	public void setRegion(String region) {
		this.region = region;
	}
	
}
