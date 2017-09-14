package fastweb.udap.web.cloudxns.action;

import java.util.ArrayList;
import java.util.TreeMap;
import java.util.List;
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
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import com.opensymphony.xwork2.ActionSupport;
import fastweb.udap.util.ESUtil;
import fastweb.udap.util.EsSearchUtil;
import fastweb.udap.web.EsClientFactory;


@Namespace("/")
@Action(value = "userresolverank", results = {
		@Result(name = ActionSupport.SUCCESS, type = "json", params = {
				"excludeProperties", "message" }),
		@Result(name = ActionSupport.ERROR, type = "json", params = {
				"includeProperties", "message" }) })
public class UserResolveRankAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "cdnlog.cloudxns";
	private static final Long DAY =  86400L;

	private String userid;
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
			AbstractAggregationBuilder subBuilder = null;

			String[] existsIndices = ESUtil.getIndicesFromMonth(begin,end+DAY,INDEX);

			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);

			allQuery = QueryBuilders.boolQuery().must(timestampQuery);
			
			//判断主机  为 all 表示查询所有主机   为@查询@主机 为空查询所有


			
			if (userid != null && !userid.equals("all")) {
				String[] useridArray = userid.split(";");
				allQuery = allQuery.must(QueryBuilders.inQuery("userid",useridArray));
			}




			AbstractAggregationBuilder viewcount = AggregationBuilders.sum("count").field("count");


//			subBuilder = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(viewcount).size(Integer.MAX_VALUE);



			if (userid != null) {
				subBuilder = AggregationBuilders.terms("userid").field("userid")
						.subAggregation(viewcount).size(Integer.MAX_VALUE).order(Terms.Order.aggregation("count",true));
			}




			searchRequestBuilder = client.prepareSearch(existsIndices).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(subBuilder);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());


			SearchResponse response = searchRequestBuilder.execute().actionGet();
			
//			System.out.println(response);

			TreeMap TreeData =  (TreeMap) EsSearchUtil.getJsonResult(response, EsSearchUtil.SearchResultType.AGGREGATIONS);

			result.add(TreeData);
				
				


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


	public void setUserid(String userid) {
		this.userid = userid;
	}
	


}
