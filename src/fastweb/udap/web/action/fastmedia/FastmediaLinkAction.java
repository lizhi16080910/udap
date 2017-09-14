package fastweb.udap.web.action.fastmedia;

import java.util.Iterator;
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

import fastweb.udap.web.EsClientFactory;

/**
 * 流媒体数据监控   链路查询
 */
@Namespace("/")
@Action(value = "fastmedialink", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaLinkAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private Map<String, String> result = new TreeMap<String, String>();
	public static final String INDEX = "fastmedia_details";
	private String message;
	private String platform;
	private String hostname;
	private String stream;
	private String id;
	private long time;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + platform + "\t" + hostname + "\t" + stream + "\t" + time);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();

			QueryBuilder timestampQuery = QueryBuilders.termQuery("timestamp", this.time);
			QueryBuilder idQuery = QueryBuilders.termQuery("id", this.id);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery).must(idQuery);
			
			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(5);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			SearchResponse response = searchRequestBuilder.execute().actionGet();

			Iterator<SearchHit> it = response.getHits().iterator();
			
			while (it.hasNext()) {
				SearchHit sh = it.next();
				Map<String, Object> source = sh.getSource();
				//null:0，publish:1，play:2，pull:3
				/**
f01.c01 播放在play的时候是用户ip  在pull 的时候会是父层ip
f01.p02 在pull的时候是f01.c01播流ip,
f01.s01 在pull的时候是客户ip （有直接从s01超父回拉客户源站情况）
f01.i01 push 的时候是超父地址，或者功能性平台地址
我说的这些针对address
address 的ip不固定 要看command动作 是play 还是pull 
client 去请求 f01.c01 播放 会产生一条play的数据，  同时f01.c01还会产生pull 去找父层去拉流
这么说吧， 用户访问一次 ，在f01.c01有两条日志分别play和pull     在f01.p02也会有两条日志play和pull ，  
 在f01.s01有publish，    f01.i01  push 和 publish   <-  企业推
				 */
				System.out.println(source.get("id") + "\t" + source.get("address") + "\t" + source.get("localaddr") + "\t" + source.get("platform") + "\t" + source.get("pull_play_publish"));
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
		if (StringUtils.isBlank(id)) {
			setMessage("链路id不能为空");
		}
	}

	public Map<String, String> getResult() {
		return result;
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

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public void setId(String id) {
		this.id = id;
	}
}
