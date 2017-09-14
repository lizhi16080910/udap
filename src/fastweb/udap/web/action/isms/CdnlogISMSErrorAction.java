package fastweb.udap.web.action.isms;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.IpListUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * isms违法网站监测统计------目前源站IP没数据，
 */
@Namespace("/")
@Action(value = "cdnlogismserror", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class CdnlogISMSErrorAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "cdnlog.isms.error";

	/* 时间戳 */
	private long time;
	/* 域名 */
	private String domain;
	/* 源站ip */
	private String sourceip;

	private String message;

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}

		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			QueryBuilder domainQuery = QueryBuilders.termQuery("domain", this.domain);

			QueryBuilder ipQuery = QueryBuilders.termQuery("ip", IpListUtil.getHostname(sourceip));

			QueryBuilder timeQuery = QueryBuilders.termQuery("time", this.time);

			QueryBuilder allQuery = QueryBuilders.boolQuery().must(domainQuery).must(timeQuery).must(ipQuery);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();
			Iterator<SearchHit> it = response.getHits().iterator();
			while (it.hasNext()) {
				SearchHit sh = it.next();
				result.add(sh.getSource());
			}
		} catch (Exception e) {
			log.error("Query Fail: " + e);
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

	public void setTime(long time) {
		this.time = time;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getMessage() {
		return message;
	}

	public void setSourceip(String sourceip) {
		this.sourceip = sourceip;
	}
}
