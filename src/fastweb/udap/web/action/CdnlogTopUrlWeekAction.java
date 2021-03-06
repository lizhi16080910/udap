package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.ServletActionContext;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * @author chenm 银汉游戏,尚娱软件--定制一周topurl 1000排序
 */
@Namespace("/")
@Action(value = "cdnlogtopurlweek", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {}) })
public class CdnlogTopUrlWeekAction extends ActionSupport {
	private static final long serialVersionUID = 1L;
	private final Log log = LogFactory.getLog(getClass());
	public static final String INDEX = "cdnlog.topurl.week";

	/* 时间戳 */
	private String time;
	/* 域名 */
	private String domain;
	/* 每页显示条数 */
	private int size = 10;
	/* 开始的记录数 */
	private int from = 0;
	/*条件下总条数*/
	private long total;

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

	@Override
	public String execute() throws Exception {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {

			client = EsClientFactory.createClient();
			QueryBuilder domainQuery = QueryBuilders.termQuery("domain", this.domain);

			QueryBuilder timeQuery = QueryBuilders.termQuery("time", this.time);

			QueryBuilder allQuery = QueryBuilders.boolQuery().must(domainQuery).must(timeQuery);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).addSort("sortRank", SortOrder.ASC).setFrom(this.from).setSize(this.size);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			SearchResponse response = searchRequestBuilder.execute().actionGet();

			/* 获取符合条件的总条数 */
			this.setTotal(response.getHits().totalHits());
			Iterator<SearchHit> it = response.getHits().iterator();
			while (it.hasNext()) {
				SearchHit sh = it.next();
				result.add(sh.getSource());
			}
		} catch (RuntimeException e) {
			e.printStackTrace();
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
	}

	public List<Map<String, Object>> getResult() {
		return result;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setTime(String time) {
		this.time = time;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}
}
