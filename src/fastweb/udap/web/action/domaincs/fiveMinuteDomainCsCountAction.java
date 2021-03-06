package fastweb.udap.web.action.domaincs;

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

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.EsSearchUtil;
import fastweb.udap.web.EsClientFactory;


@Namespace("/")
@Action(value = "fiveminutecscount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class fiveMinuteDomainCsCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "cdnlog.domain.flow.size.count";
	// private static final Long DAY = 86400L;

	private String domain;
	private String userid;
	private String https_flash;
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

			BoolQueryBuilder allQuery = null;
			AbstractAggregationBuilder subBuilder = null;

			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);

			allQuery = QueryBuilders.boolQuery().must(timestampQuery);

			if (domain != null && !domain.equals("all")) {
				String[] domainArray = domain.split(";");
				allQuery = allQuery.must(QueryBuilders.inQuery("domain", domainArray));
			}

			if (userid != null && !userid.equals("all")) {
				String[] useridArray = userid.split(";");
				allQuery = allQuery.must(QueryBuilders.inQuery("userid", useridArray));
			}

			if(StringUtils.isNotBlank(https_flash)){
				allQuery.must(QueryBuilders.inQuery("https_flash", https_flash));
			}
			
			
			AbstractAggregationBuilder csSum = AggregationBuilders.sum("flow_size").field("flow_size");
			subBuilder = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(csSum).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(subBuilder);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			@SuppressWarnings("unchecked")
			TreeMap<String, Long> TreeData = (TreeMap<String, Long>) EsSearchUtil.getJsonResult(response, EsSearchUtil.SearchResultType.AGGREGATIONS);

			result.add(TreeData);
		} catch (Exception e) {
			this.message = "请求失败";
			log.error("Query Fail: " + e);
			return ActionSupport.ERROR;
		} finally {
			if (client != null) {
				client.close();
			}
		}
		return ActionSupport.SUCCESS;
	}

	@Override
	public void validate() {
		super.validate();
		if (begin > end) {
			setMessage("开始时间戳大于结束时间戳");
		}
		if (end - begin > 31 * 24 * 60 * 1000) {
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

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public void setHttps_flash(String https_flash) {
		this.https_flash = https_flash;
	}
}
