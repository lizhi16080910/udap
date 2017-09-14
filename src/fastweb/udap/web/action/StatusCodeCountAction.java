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
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.ESUtil;
import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * @author chenm 状态码数量
 */
@Namespace("/")
@Action(value = "statuscodecount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
		   @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class StatusCodeCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private static String indexPrefix = "cdnlog.status.code";
	/* 开始时间戳 */
	private long begin;
	/* 结束时间戳 */
	private long end;

	private String metric = MetricConst.STATUS_CODE_DOMAIN_ONE.getMetricStr();
	
	/* 域名 */
	private String domain;
	/* 域名 */
	private String isp = null;
	/* 条件下总条数 */
	// private long total = 0;
	private String userid = null;
	/* 省份*/
	private String prv = null;
	
	private String message;
	
	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

	@Override
	public String execute() throws Exception {

		if(!StringUtil.isEmpty(message)) {
			return ActionSupport.ERROR;
		}
		Client client = null;
		try {
			client = EsClientFactory.createClient();
		
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
			
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);
			
			/* when domain equals "all", use userid to search for all domains thats owned by this userid */
			if( domain.equalsIgnoreCase("all") && null != userid) {
				allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			} else{
				allQuery = allQuery.must(QueryBuilders.queryString(
						this.domain).field("domain"));
			}
			/* has isp parameter */
			if(metric.startsWith("status.code.domain.isp")){
				if(null != isp && !isp.equalsIgnoreCase("all") ) {
					allQuery = allQuery.must(QueryBuilders.termQuery("isp", this.isp));
				}
				if( null != prv && !prv.equalsIgnoreCase("all")) {
					allQuery = allQuery.must(QueryBuilders.termQuery("prv", this.prv));
				}
			}
			/*index 存在性检*/
			String[] existsIndices = ESUtil.getExistIndices(getIndicesByMetric()).toArray( new String[0]);
			if( 0 ==  existsIndices.length) { /*不存在index*/
				setMessage("No index for this time span exists!");
				return ActionSupport.ERROR;
			}
			
			SearchRequestBuilder searchBuilder = client.prepareSearch(existsIndices)
					.setQuery(allQuery);
			
	
			/* build the aggregation */
			String metricFieldName = getMetricFieldByMetric();
			
			AbstractAggregationBuilder metricFieldAgg = AggregationBuilders.sum(metricFieldName).field(metricFieldName);
			String aggFieldName = getAggFieldByMetric();
			TermsBuilder aggFieldAgg = AggregationBuilders.terms(aggFieldName).field(aggFieldName).size(Integer.MAX_VALUE);
			TermsBuilder statusCodeAgg = AggregationBuilders.terms("status_code").field("status_code").subAggregation(metricFieldAgg).size(Integer.MAX_VALUE);
			/* 2级分类 */
			SearchResponse searchResponse = searchBuilder.addAggregation(aggFieldAgg.subAggregation(metricFieldAgg).subAggregation(statusCodeAgg))
						.setFrom(0).setSize(0)
						.execute().actionGet();
			generateFinalResult(searchResponse,0);
		

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
		}
		return ActionSupport.SUCCESS;
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
	
	public void setPrv(String prv) {
		this.prv = prv;
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

	private void generateFinalResult(SearchResponse searchResponse, int type) {
		if(0 == type) {
			String  aggFieldName = getAggFieldByMetric();
	        Terms aggFieldTerm = searchResponse.getAggregations().get(aggFieldName);
	        List<Bucket> bucketList = aggFieldTerm.getBuckets();
	        Map<String,Object> aggMap = new HashMap<String,Object>();
	        List<Object> keyValueList = new ArrayList<Object>();
	        aggMap.put(aggFieldName, keyValueList);
	        String metricFieldName = getMetricFieldByMetric();
	        for(Bucket buck:bucketList) {
				Map<String,Map<String,Long>> keyValueTurple = new TreeMap<String,Map<String,Long>>();
				Terms statusCodeAgg = buck.getAggregations().get("status_code");
				
				List<Bucket> statusCodebucketList = statusCodeAgg.getBuckets();
				Map<String,Long> statusCodeKeyValue = new TreeMap<String,Long>();
				for(Bucket statusCodeBuck:statusCodebucketList) {
					Sum tmpAgg = statusCodeBuck.getAggregations().get(metricFieldName);
					statusCodeKeyValue.put(statusCodeBuck.getKey(),((Double)tmpAgg.getValue()).longValue());	
				}
				if(!statusCodeKeyValue.isEmpty()) {
					keyValueTurple.put(buck.getKey(),statusCodeKeyValue);
					keyValueList.add(keyValueTurple);
				}
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
		if(metric.matches("status\\.code\\.domain\\.(one|five|hour)")){
			return "timestamp";
		} else if(metric.startsWith("status.code.domain.isp")) {
			if(null == isp) {
				return "isp";
			} else {
				if(null == prv){
					return "prv";
				} else {
					return "city";
				}
			}
		} else if(metric.startsWith("status.code.domain.machine")){
			return "isp";
		}  else {
			return "isp";
		}
	}

	public List<Map<String, Object>> getResult() {
		return result;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setBegin(long begin) {
		this.begin = begin ;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public void setIsp(String isp) {
		this.isp = isp;
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

	@Override
	public void validate() {
		super.validate();
		if(StringUtil.isEmpty(domain)){
			setMessage("参数domain为空");
		} else if(begin > end)
		{
			setMessage("开始时间戳大于结束时间戳");
		} else if(!MetricConst.getAllMetricStr().contains(metric)){
			setMessage("metric参数值不在给定列表中");
		}
	}
	
	public enum MetricConst {	
		STATUS_CODE_DOMAIN_ISP_ONE("status.code.domain.isp.one"), STATUS_CODE_DOMAIN_ISP_FIVE("status.code.domain.isp.five"),STATUS_CODE_DOMAIN_ISP_HOUR("status.code.domain.isp.hour"),
		STATUS_CODE_DOMAIN_ONE("status.code.domain.one"), STATUS_CODE_DOMAIN_FIVE("status.code.domain.five"), STATUS_CODE_DOMAIN_HOUR("status.code.domain.hour"),
		STATUS_CODE_DOMAIN_MACHINE_ONE("status.code.domain.machine.one"), STATUS_CODE_DOMAIN_MACHINE_FIVE("status.code.domain.machine.five"),STATUS_CODE_DOMAIN_MACHINE_HOUR("status.code.domain.machine.hour");
		
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
