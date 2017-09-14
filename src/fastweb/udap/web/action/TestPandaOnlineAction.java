package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.xwork.StringUtils;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

@Namespace("/")
@Action(value = "testpandaonline", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class TestPandaOnlineAction extends ActionSupport {
	public static final String INDEX = "pandatv_online";
	private static final long serialVersionUID = 1L;
	private String domain;
	private String stream;
	private String platform;
	private String app;
	private long begin;
	private long end;
	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
	private String message;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		try {
			Client client = EsClientFactory.createClient();
			try {
				QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
				QueryBuilder domainQuery = QueryBuilders.termQuery("domain", this.domain);
				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery).must(domainQuery);

				if (StringUtils.isNotBlank(app)) {
					QueryBuilder appQuery = QueryBuilders.termQuery("app", this.app);
					allQuery.must(appQuery);

				}
				if (StringUtils.isNotBlank(stream)) {
					QueryBuilder streamQuery = QueryBuilders.termQuery("stream", this.stream);
					allQuery.must(streamQuery);
				}
				if (StringUtils.isNotBlank(platform)) {
					String p = "*";
					String[] platforms = platform.split(",");
					Arrays.sort(platforms);
					for (int i = 0; i < platforms.length; i++) {
						p += platforms[i] + "*";
					}
					QueryBuilder platformQuery = QueryBuilders.queryString(p).field("platform");
					allQuery.must(platformQuery);

				}
				SearchResponse response = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(2000).execute().actionGet();
				Iterator<SearchHit> it = response.getHits().iterator();
				while (it.hasNext()) {
					SearchHit sh = it.next();
					result.add(sh.getSource());
				}
			} catch (RuntimeException e) {
				e.printStackTrace();
			} finally {
				client.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			this.message = "请求错误";
			return ActionSupport.ERROR;
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
			setMessage("开始时间戳大于结束时间戳");
		}
	}

	public List<Map<String, Object>> getResult() {
		return result;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public void setApp(String app) {
		this.app = app;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}
}
