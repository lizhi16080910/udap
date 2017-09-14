package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.EsSearchUtil;
import fastweb.udap.util.ESUtil;
import fastweb.udap.util.StringUtil;
import fastweb.udap.util.Time2Type;
import fastweb.udap.web.EsClientFactory;

/**
 * @author yecg
 */
@Namespace("/")
@Action(value = "copyflowsizecount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
		   @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class CopyOfFlowSizeCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;
	
	private String metric = MetricConst.FLOW_HIT_COUNT_ONE.getMetricStr();
	
	static private String indexPrefix = "cdnlog.flow.size.count";
	/* 域名*/
	private String domain;
	/* 查询开始时间戳*/
	private long begin;
	/* 查询结束时间戳*/
	private long end;
	/* 省份信息 */
	private String prv = null;
	/* 运营提供商 */
	private String isp = null;
	/* 用户标识 */
	private String user_id = null;
	
	private String message;
	
	private List<Object> result = new ArrayList<Object>();
	
	@Override
	public String execute() throws Exception {
		if(!StringUtil.isEmpty(message)) {
			return ActionSupport.ERROR;
		}
		
		try {

			Client client = EsClientFactory.createClient();
			try {
				/* construct query */
				QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);
				if(domain.equalsIgnoreCase("all") && null != user_id ) {
					allQuery = allQuery.must(QueryBuilders.termQuery("userid",this.user_id));
				} else {
					allQuery = allQuery.must(QueryBuilders.queryString(this.domain).field("domain"));
				}
				
				/* isp和prv参数的多种组合可以对应多种不同的查询*/
				if(metric.startsWith("flow.isp.prv.count")){
					if( null != isp && !isp.equalsIgnoreCase("all")) {
						allQuery = allQuery.must(QueryBuilders.termQuery("isp",
								this.isp));
					}
					if(null != prv && !prv.equalsIgnoreCase("all")) {
						allQuery = allQuery.must(QueryBuilders.termQuery("prv",
								this.prv));
					}
				}
				
				/*index 存在性检*/
				String[] existsIndices = ESUtil.getExistIndices(getIndicesByMetric()).toArray( new String[0]);
				if( 0 ==  existsIndices.length) { /*不存在index*/
					setMessage("No index for this time span exists!");
					return ActionSupport.ERROR;
				}
				
				SearchRequestBuilder searchBuilder = client.prepareSearch(existsIndices).setQuery(allQuery);
				
				String metricFieldName = getMetricFieldByMetric();
				AbstractAggregationBuilder metricFieldAgg = AggregationBuilders.sum(metricFieldName).field(metricFieldName);
				String aggFieldName = getAggFieldByMetric();
				SearchResponse searchResponse = searchBuilder.addAggregation(AggregationBuilders.terms(aggFieldName).field(aggFieldName).size(Integer.MAX_VALUE).subAggregation(metricFieldAgg))
							.setFrom(0).setSize(0)
							.execute().actionGet();
				result.add(EsSearchUtil.getJsonResult(searchResponse, EsSearchUtil.SearchResultType.AGGREGATIONS));
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

	/*根据metric获取需要group by的字段名*/
	private String getAggFieldByMetric() {
		if(metric.startsWith("flow.count")) {
			return "timestamp";
		} else if(metric.startsWith("flow.hit.count")) {
			return "hit_type";
		} else if( metric.startsWith("flow.isp.prv.count")) {
			if( null == isp ) {
				return "isp";
			} else {
				if(null == prv) {/*传进来的prv参数为省份*/
					return "prv"; /*按照省份分组统计(area_pid在Es中为省份字段名)*/
				} else  {
					return "city"; /*按照城市分组统计(prv在Es中为城市字段名)*/
				}
			}
		}
		return "hit_type";
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
	
	/*根据metric获取需要统计的变量名*/
	private String getMetricFieldByMetric() {
		return "flow_size";
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
	
	public void setMetric(String metric) {
		this.metric = metric;
	}
	
	public List<Object> getResult()
	{
		return result;
	}
	
	public void setIsp(String isp) 
	{
		this.isp = isp;
	}
	
	public void setPrv(String prv) {
		this.prv = prv;
	}


	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}
	
	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
	
	
	/* 参数校验*/
	@Override
	public void validate() {
		super.validate();
		if(StringUtil.isEmpty(domain)){
			setMessage("参数domain为空");
			return;
		} 
		
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
	
	/*流量查询支持的所有metric*/
	public enum MetricConst {
		FLOW_COUNT_ONE("flow.count.one"), FLOW_COUNT_FIVE("flow.count.five"), FLOW_COUNT_HOUR("flow.count.hour"),/*流量按时间统计*/
		FLOW_HIT_COUNT_ONE("flow.hit.count.one"), FLOW_HIT_COUNT_FIVE("flow.hit.count.five"), FLOW_HIT_COUNT_HOUR("flow.hit.count.hour"),/*流量安回源情况统计*/
		FLOW_ISP_PRV_COUNT_ONE("flow.isp.prv.count.one"), FLOW_ISP_PRV_COUNT_FIVE("flow.isp.prv.count.five"), FLOW_ISP_PRV_COUNT_HOUR("flow.isp.prv.count.hour"),/*流量按isp统计*/
		FLOW_MACHINE_PRV_COUNT_ONE("flow.machine.prv.count.one"), FLOW_MACHINE_PRV_COUNT_FIVE("flow.machine.prv.count.five"), FLOW_MACHINE_PRV_COUNT_HOUR("flow.machine.prv.count.hour"),/*机器视图prv总和统计*/
		FLOW_MACHINE_TOPN_ONE("flow.machine.topn.one"),/*实时按带宽取前20域名*/
		FLOW_MACHINE_ISP_PRV_COUNT_FIVE("flow.machine.isp.prv.count.five"), FLOW_MACHINE_ISP_PRV_COUNT_HOUR("flow.machine.isp.prv.count.hour");/*全量机器视图*/
		
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

