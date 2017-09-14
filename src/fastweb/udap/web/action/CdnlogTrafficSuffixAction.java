package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.List;

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
import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * 后缀流量
 */
@Namespace("/")
@Action(value = "cdnlogtrafficsuffix", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
		   @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class CdnlogTrafficSuffixAction extends ActionSupport {
	private static final long serialVersionUID = 1L;
	
	private static final String INDEX = "cdnlog.traffic.suffix";
	/*域名*/
	private String domain;
	/*开始时间戳*/
	private long start;
	/*结束时间戳*/
	private long end;
	/*错误信息*/
	private String message;
	/*后缀*/
	private String suffix = null;
	/*userid*/
    private String userid = null;

	private List<Object> result = new ArrayList<Object>();
	
	@Override
	public String execute() throws Exception {
		if(!StringUtil.isEmpty(message)) {
			return ActionSupport.ERROR;
		}
		try {
			Client client = EsClientFactory.createClient();
			try {
				QueryBuilder timeQuery = QueryBuilders.rangeQuery("time").from(start).to(end);
				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timeQuery);
				if(!"*".equals(suffix)){
					allQuery = allQuery.must(QueryBuilders.termQuery("suffix",this.suffix));
				}
				if(domain.equalsIgnoreCase("all") && null != userid ) {
					allQuery = allQuery.must(QueryBuilders.termQuery("userid",this.userid));
				} else {
					allQuery = allQuery.must(QueryBuilders.queryString(this.domain).field("domain"));
				}
				SearchRequestBuilder searchBuilder = client.prepareSearch(INDEX).setQuery(allQuery);
				
				/* build the aggregation */
				AbstractAggregationBuilder fieldAgg = AggregationBuilders.sum("traffic").field("traffic");
				AbstractAggregationBuilder count = AggregationBuilders.sum("count").field("count");
				String aggFieldName = "suffix";
				AbstractAggregationBuilder builder = AggregationBuilders.terms(aggFieldName).field(aggFieldName).size(Integer.MAX_VALUE).subAggregation(fieldAgg).subAggregation(count);
				searchBuilder = searchBuilder.addAggregation(builder);
				SearchResponse searchResponse = searchBuilder.setFrom(0).setSize(0).execute().actionGet();
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

	public void setDomain(String domain) {
		this.domain = domain;
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
	
	public void setStart(long start) {
		this.start = start;
	}

	public void setSuffix(String suffix) {
		this.suffix = suffix;
	}

	public void setMessage(String message) {
		this.message = message;
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
		if(start > end)
		{
			setMessage("开始时间戳大于结束时间戳");
		}
	}
}

