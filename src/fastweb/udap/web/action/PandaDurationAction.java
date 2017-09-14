package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.elasticsearch.search.SearchHit;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * 熊猫tv在线时长查询
 */
@Namespace("/")
@Action(value = "pandaduration", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class PandaDurationAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());
	public static final String INDEX = "pandatv.online.duration";
	private String domain;
	private String stream;
	private String node;
	private String sign;
	private String app;
	private int from = 0;
	private int size = Integer.MAX_VALUE;
	private long time;
	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
	private String message;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + time + "\t" + domain + "\t" + node + "\t" + app + "\t" + stream + "\t" + sign);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			
			QueryBuilder timestampQuery = QueryBuilders.termQuery("time", this.time);
			QueryBuilder domainQuery = QueryBuilders.termQuery("domain", this.domain);
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery).must(domainQuery);
			if (StringUtils.isNotBlank(app)) {
				QueryBuilder appQuery = QueryBuilders.termQuery("appName", this.app);
				allQuery.must(appQuery);
			}
			if (StringUtils.isNotBlank(stream)) {
				QueryBuilder streamQuery = QueryBuilders.termQuery("streamName", this.stream);
				allQuery.must(streamQuery);
			}
			if (StringUtils.isNotBlank(node)) {
				QueryBuilder nodeQuery = QueryBuilders.termQuery("node", this.node);
				allQuery.must(nodeQuery);
			}
			if (StringUtils.isNotBlank(sign)) {
				QueryBuilder signQuery = QueryBuilders.termQuery("sign", this.sign);
				allQuery.must(signQuery);
			}
			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(this.from).setSize(this.size);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			
			SearchResponse response = searchRequestBuilder.execute().actionGet();

			Iterator<SearchHit> it = response.getHits().iterator();
			while (it.hasNext()) {
				SearchHit next = it.next();
				result.add(next.getSource());
			}
		} catch (RuntimeException e) {
			log.error("Query Fail: " + e);
			this.message = "请求错误";
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
		if (StringUtil.isEmpty(String.valueOf(time))) {
			setMessage("开始时间戳大于结束时间戳");
		}
	}

	public List<Map<String, Object>> sumDuration() {
		List<Map<String, Object>> result_ = new ArrayList<Map<String, Object>>();
		Map<String, Long> sum_time = new TreeMap<String, Long>();
		// 转换result数据
		for (Map<String, Object> key : result) {
			String domain = (String) key.get("domain");
			String app = (String) key.get("app");
			String stream = (String) key.get("stream");
			String node = (String) key.get("node");
			Long durationTime = Long.parseLong(key.get("durationTime").toString()) > 0 ? Long.parseLong(key.get("durationTime").toString()) : 0L;
			if (sum_time.containsKey(domain + "/" + app + "/" + stream + "/" + node)) {
				Long newDurationTime = sum_time.get(domain + "/" + app + "/" + stream + "/" + node) + durationTime;
				sum_time.put(domain + "/" + app + "/" + stream + "/" + node, newDurationTime);
			} else {
				sum_time.put(domain + "/" + app + "/" + stream + "/" + node, durationTime);
			}
		}
		for (Map<String, Object> key : result) {
			String domain = (String) key.get("domain");
			String app = (String) key.get("app");
			String stream = (String) key.get("stream");
			String node = (String) key.get("node");
			Long sum = sum_time.get(domain + "/" + app + "/" + stream + "/" + node);
			key.remove("node");
			key.remove("sign");
			key.remove("time");
			key.put("sum_time", sum);
			result_.add(key);
		}
		return result_;
	}

	public List<Map<String, Object>> getResult() {
		return sumDuration();
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setTime(long time) {
		this.time = time;
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

	public void setNode(String node) {
		this.node = node;
	}

	public void setSign(String sign) {
		this.sign = sign;
	}
}
