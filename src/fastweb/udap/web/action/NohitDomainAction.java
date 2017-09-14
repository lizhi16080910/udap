package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * 回源
 * 
 * @author shuzhangyao
 */
@Namespace("/")
@Action(value = "nohitdomain", results = {@Result(name = ActionSupport.SUCCESS, type = "json", params = {"excludeProperties","message"}),
	   									  @Result(name = ActionSupport.ERROR, type = "json", params={"includeProperties","message"})})
public class NohitDomainAction extends ActionSupport {

	private static final long serialVersionUID = 1L;

	public static final String INDEX = "nohit.domain.count";
	/**
	 * 1分钟 父层命中的域名视图
	 */
	public static final String BACK_PARENT_HIT_URL_ONE = "back.parent.hit.url.one";

	/**
	 * 5分钟 父层命中的域名视图
	 */
	public static final String BACK_PARENT_HIT_URL_FIVE = "back.parent.hit.url.five";

	/**
	 * 1小时 父层命中的域名视图
	 */
	public static final String BACK_PARENT_HIT_URL_HOUR = "back.parent.hit.url.hour";
	/**
	 * 1分钟 回源域名视图
	 */
	public static final String BACK_SOURCE_URL_COUNT_ONE = "back.source.url.count.one";

	/**
	 * 5分钟 回源域名视图
	 */
	public static final String BACK_SOURCE_URL_COUNT_FIVE = "back.source.url.count.five";

	/**
	 * 1小时 回源域名视图
	 */
	public static final String BACK_SOURCE_URL_COUNT_HOUR = "back.source.url.count.hour";

	/**
	 * 1分钟 回父层未命中域名视图
	 */
	public static final String BACK_PARENT_MISS_URL_ONE = "back.parent.miss.url.one";

	/**
	 * 5分钟 回父层未命中域名视图
	 */
	public static final String BACK_PARENT_MISS_URL_FIVE = "back.parent.miss.url.five";

	/**
	 * 1小时 回父层未命中域名视图
	 */
	public static final String BACK_PARENT_MISS_URL_HOUR = "back.parent.miss.url.hour";

	private static final long ONE_DAY = 24 * 60 * 60 * 1000l;
	
	/* 时间戳 */
//	private long timestamp;
	
	/*时间戳开始*/
	private long begin;
	/*时间戳结束*/
	private long end;
	
	/* 域名 */
	private String domain;
	/* 每页显示条数 */
	private int size = 10;
	/* 开始的记录数 */
	private int from = 0;
	/**
	 * 1：父层命中，2：回源，3：父层未命中
	 */
	private int hitType = 1;

	private String metric = BACK_PARENT_HIT_URL_ONE;

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

	private String message;
	
	
	@Override
	public String execute() throws Exception {
		try {
			/*时间戳要小于24小时*/
			if(this.end-this.begin <= ONE_DAY/1000){
				Client client = EsClientFactory.createClient();
				try {
					QueryBuilder domainQuery = QueryBuilders.termQuery("domain",
							this.domain);
					
					QueryBuilder typeQuery = QueryBuilders.termQuery("hit_type",
							this.hitType);
					
					/*QueryBuilder timestampQuery = QueryBuilders.termQuery(
						"timestamp", this.timestamp);*/
					
					QueryBuilder metricQuery = QueryBuilders.termQuery("metric",
							this.metric);
					
					QueryBuilder allQuery = QueryBuilders.boolQuery().must(
							domainQuery).must(typeQuery)./*must(timestampQuery).*/must(
									metricQuery);
					
					SearchResponse response = client.prepareSearch(INDEX).setQuery(
							allQuery).addSort("timestamp", SortOrder.DESC).setPostFilter(FilterBuilders.rangeFilter("timestamp").from(begin).to(end)).setFrom(from).setSize(size)
							.execute().actionGet();
					
					Iterator<SearchHit> it = response.getHits().iterator();
					while (it.hasNext()) {
						SearchHit sh = it.next();
						result.add(sh.getSource());
					}
				} finally {
					client.close();
				}
			}else{
				this.setMessage("时间戳大于24小时，请确认！");
				return ActionSupport.ERROR;
			}

		} catch (Exception e) {
		} finally {
		}
		return ActionSupport.SUCCESS;
	}

	public List<Map<String, Object>> getResult() {
		return result;
	}

	/*public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}*/

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public void setHitType(int hitType) {
		this.hitType = hitType;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
}
