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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * @author chenm 错误请求数-小时
 */
@Namespace("/")
@Action(value = "cdnlogonlinemembercountaction", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {}) })
public class CdnlogOnlineMemberCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	public static final String INDEX = "cdnlog.online.count";

	/* 时间戳 */
	private String time;
	/* 域名 */
	private String url;
	
	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	private String status;
	private String start;
	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

	private String end;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	/* 每页显示条数 */
	//private int size = 10;
	/* 开始的记录数 */
	private int from = 0;
	/*条件下总条数*/
	private long total;

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

	@Override
	public String execute() throws Exception {
		Client client = null;

		try {

			client = EsClientFactory.createClient();
			QueryBuilder domainQuery = QueryBuilders.termQuery("url", this.url);

			QueryBuilder timeQuery = QueryBuilders.rangeQuery("time").from(start).to(end);
			
			QueryBuilder statusQuery = QueryBuilders.termQuery("status", this.status);
			
			//QueryBuilder hourQuery = QueryBuilders.rangeQuery("hour").from(startHour).to(termHour);
			 
			

			QueryBuilder allQuery = QueryBuilders.boolQuery().must(domainQuery).must(timeQuery);//.must(hourQuery);

			SearchResponse response = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(this.from)/*.setSize(this.size)*/.execute().actionGet();
			/* 获取符合条件的总条数 */
			
			this.setTotal(response.getHits().totalHits());
			Iterator<SearchHit> it = response.getHits().iterator();
			while (it.hasNext()) {
				SearchHit sh = it.next();
				result.add(sh.getSource());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			client.close();
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

	 

	/*public void setSize(int size) {
		this.size = size;
	}*/

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
