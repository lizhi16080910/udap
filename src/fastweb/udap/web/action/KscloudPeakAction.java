package fastweb.udap.web.action;

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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * 域名,运营商,区域的带宽峰值表(五分钟峰值);
 */
@Namespace("/")
@Action(value = "kscloudpeak", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class KscloudPeakAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());
	
	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
	public static final String INDEX = "peak";
	private String message;
	private String domain;
	private String prv;
	private String isp;
	private int size = Integer.MAX_VALUE;
	private int from = 0;
	private long begin;
	private long end;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("time").from(this.begin).to(this.end);
			QueryBuilder domainQuery = QueryBuilders.termQuery("domain", this.domain);
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery).must(domainQuery);

			if (StringUtils.isNotBlank(isp)) {
				QueryBuilder ispQuery = QueryBuilders.termQuery("isp", this.isp);
				allQuery.must(ispQuery);
			}
			if (StringUtils.isNotBlank(prv)) {
				QueryBuilder prvQuery = QueryBuilders.termQuery("prv", this.prv);
				allQuery.must(prvQuery);
			}
			
			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(this.from).setSize(this.size);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			
			SearchResponse response = searchRequestBuilder.execute().actionGet();

			Iterator<SearchHit> it = response.getHits().iterator();
			while (it.hasNext()) {
				SearchHit sh = it.next();
				result.add(sh.getSource());
			}
		} catch (RuntimeException e) {
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
		if (StringUtil.isEmpty(domain)) {
			setMessage("域名不能为空。");
		}
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
		}
	}

	public List<Map<String, Object>> getResult() {
		return result;
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

	public void setMessage(String message) {
		this.message = message;
	}

	public String getMessage() {
		return message;
	}

	public void setPrv(String prv) {
		this.prv = prv;
	}

	public void setIsp(String isp) {
		this.isp = isp;
	}
}
