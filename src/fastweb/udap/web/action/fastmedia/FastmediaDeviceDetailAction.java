package fastweb.udap.web.action.fastmedia;

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

import fastweb.udap.web.EsClientFactory;

/**
 * 流媒体数据监控   设备详情查看
 */
@Namespace("/")
@Action(value = "fastmediadevicedetail", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaDeviceDetailAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
	public static final String INDEX = "fastmedia_details";
	private String message;
	//时间段、流名称、域名、IP、平台
	private String platform;
	private String domain;
	private String stream;
	private String id;
	private String localaddr;
	private String address;
	
	private String prv;
	private String hostname;
	private String isp;
	
	private long total = 0;
	private long time = 0;
	private int from = 0;
	private int size = 20;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + platform + "\t" + address + "\t" + stream + "\t" + time + "\t" + domain + "\t" + localaddr + "\t" + id);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();

			QueryBuilder timestampQuery = QueryBuilders.termQuery("timestamp", this.time);
			
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);

			if(StringUtils.isNotBlank(id)){
				QueryBuilder idQuery = QueryBuilders.termQuery("id", this.id);
				allQuery.must(idQuery);
			}
			if(StringUtils.isNotBlank(stream)){
				allQuery.must(QueryBuilders.termQuery("stream", this.stream));
			}
			if(StringUtils.isNotBlank(domain)){
				allQuery.must(QueryBuilders.termQuery("domain", this.domain));
			}
			if(StringUtils.isNotBlank(platform)){
				allQuery.must(QueryBuilders.termQuery("platform", this.platform));
			}
			if(StringUtils.isNotBlank(localaddr)){
				allQuery.must(QueryBuilders.termQuery("localaddr", this.localaddr));
			}
			if(StringUtils.isNotBlank(address)){
				allQuery.must(QueryBuilders.termQuery("address", this.address));
			}
			
			if(StringUtils.isNotBlank(hostname)){
				allQuery.must(QueryBuilders.termQuery("hostname", this.hostname));
			}
			
			if (StringUtils.isNotBlank(isp)) {
				allQuery.must(QueryBuilders.termsQuery("isp", this.isp.split(",")));
			}
			if (StringUtils.isNotBlank(prv)) {
				allQuery.must(QueryBuilders.termsQuery("prv", this.prv.split(",")));
			}
			
			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(from).setSize(size);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			SearchResponse response = searchRequestBuilder.execute().actionGet();

			Iterator<SearchHit> it = response.getHits().iterator();
			
			this.setTotal(response.getHits().totalHits());
			
			while (it.hasNext()) {
				SearchHit sh = it.next();
				Map<String, Object> source = sh.getSource();
				result.add(source);
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
		if (time == 0) {
			setMessage("查询时间点不能为空。");
		}
	}

	public List<Map<String, Object>> getResult() {
		return result;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getMessage() {
		return message;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public void setLocaladdr(String localaddr) {
		this.localaddr = localaddr;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setPrv(String prv) {
		this.prv = prv;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setIsp(String isp) {
		this.isp = isp;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}
}
