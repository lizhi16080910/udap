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
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.sort.SortOrder;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * fastmedia推流直播带宽，人数统计
 */
@Namespace("/")
@Action(value = "fastmediapushcount", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class FastmediaPushCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

	private Map<String, String> onlineMap = new TreeMap<String, String>();
	private Map<String, String> fcdHlsMap = new TreeMap<String, String>();

	// 推流基本信息查询
	public static final String INDEX_PUSH = "fastmedia_push";
	// 推流带宽，人数，在播流数据查询-----flv
	public static final String INDEX_ONLINE = "pandatv_online";
	// 推流带宽，人数，在播流数据查询-----hls
	public static final String INDEX_FCD_HLS = "fastmedia_fcd_hls";
	private String message;
	// 推流域名 f01.i01
	private String domain_push;
	// 播流域名 f01.c01
	private String domain_play;
	// FCD边缘加速直播点播域名
	private String domain_hls;
	private String app;
	private String stream;
	private String platform;
	private long begin;
	private long end;
	private long total = 0;
	/* 每页显示条数 */
	private int size = 20;
	/* 开始的记录数 */
	private int from = 0;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		if (initPushBasic() == 0) {
			return ActionSupport.ERROR;
		}
		if (initOnline() == 0) {
			return ActionSupport.ERROR;
		}
		if (initFcdHls() == 0) {
			return ActionSupport.ERROR;
		}
		return ActionSupport.SUCCESS;
	}

	// 初始化推流平台(f01.i01)的基本信息
	public int initPushBasic() {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("timestamp").from(begin).to(end)).must(QueryBuilders.termQuery("domain", this.domain_push)).must(QueryBuilders.termQuery("stream", this.stream));

			if (StringUtils.isNotBlank(app)) {
				allQuery.must(QueryBuilders.termQuery("app", this.app));
			}

			if (StringUtils.isNotBlank(platform)) {
				QueryBuilder platformQuery = QueryBuilders.queryString("*" + this.platform + "*").field("platform");
				allQuery.must(platformQuery);
			}

			searchRequestBuilder = client.prepareSearch(INDEX_PUSH).setQuery(allQuery).setFrom(from).setSize(size).addSort("timestamp", SortOrder.ASC);

			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			this.setTotal(response.getHits().totalHits());

			Iterator<SearchHit> it = response.getHits().iterator();
			while (it.hasNext()) {
				SearchHit sh = it.next();
				result.add(sh.getSource());
			}

		} catch (RuntimeException e) {
			this.message = "请求失败";
			log.error("Query Fail: " + e);
			return 0;
		} finally {
			if (client != null) {
				client.close();
			}
		}
		return 1;
	}

	// 初始化播流平台(f01.c01)查询数据，带宽，人数-----flv
	public int initOnline() {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("timestamp").from(begin).to(end)).must(QueryBuilders.termQuery("domain", this.domain_play));

			//.must(QueryBuilders.termQuery("stream", this.stream))
			
			if (StringUtils.isNotBlank(app)) {
				allQuery.must(QueryBuilders.termQuery("app", this.app));
			}
			
			if (StringUtils.isNotBlank(stream)) {
		//		QueryBuilder platformQuery = QueryBuilders.queryString(this.stream + "*").field("stream");
				allQuery.must(QueryBuilders.queryString(this.stream + "*").field("stream"));
			}
			
			AbstractAggregationBuilder cSum = AggregationBuilders.sum("cs").field("cs");
			AbstractAggregationBuilder countSum = AggregationBuilders.sum("nclients").field("nclients");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(cSum).subAggregation(countSum).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX_ONLINE).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(timestampKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms timestampAgge = response.getAggregations().get("timestamp");

			for (Bucket time : timestampAgge.getBuckets()) {
				Sum cs = time.getAggregations().get("cs");
				Sum count = time.getAggregations().get("nclients");
				long csValue = ((Double) cs.getValue()).longValue();
				long countValue = ((Double) count.getValue()).longValue();
				onlineMap.put(this.domain_push + "######" + this.app + "######" + this.stream + "######" + time.getKey(), csValue + "######" + countValue);
			}

		} catch (RuntimeException e) {
			this.message = "请求失败";
			log.error("Query Fail: " + e);
			return 0;
		} finally {
			if (client != null) {
				client.close();
			}
		}
		return 1;
	}

	// 初始化Fcd边缘加速(c01.i02 and c06.i06)查询数据，带宽(cs)，人数(独立ip)-----hls
	public int initFcdHls() {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.rangeQuery("timestamp").from(begin).to(end)).must(QueryBuilders.termQuery("domain", this.domain_hls)).must(QueryBuilders.termQuery("stream", this.stream));

			if (StringUtils.isNotBlank(app)) {
				allQuery.must(QueryBuilders.termQuery("app", this.app));
			}
			AbstractAggregationBuilder cSum = AggregationBuilders.sum("cs").field("cs");
			AbstractAggregationBuilder countSum = AggregationBuilders.sum("nclients").field("nclients");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(cSum).subAggregation(countSum).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX_FCD_HLS).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(timestampKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms timestampAgge = response.getAggregations().get("timestamp");

			for (Bucket time : timestampAgge.getBuckets()) {
				Sum cs = time.getAggregations().get("cs");
				Sum count = time.getAggregations().get("nclients");
				long csValue = ((Double) cs.getValue()).longValue();
				long countValue = ((Double) count.getValue()).longValue();
				fcdHlsMap.put(this.domain_push + "######" + this.app + "######" + this.stream + "######" + time.getKey(), csValue + "######" + countValue);
			}

		} catch (RuntimeException e) {
			this.message = "请求失败";
			log.error("Query Fail: " + e);
			return 0;
		} finally {
			if (client != null) {
				client.close();
			}
		}
		return 1;
	}

	// 返回结果
	public List<Map<String, Object>> getResult() {
		List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> map : result) {
			String key = this.domain_push + "######" + map.get("app") + "######" + map.get("stream") + "######" + map.get("timestamp");
			Map<String, Object> resMap = new TreeMap<String, Object>();
			resMap.put("domain", map.get("domain"));
			resMap.put("app", map.get("app"));
			resMap.put("stream", map.get("stream"));
			resMap.put("timestamp", map.get("timestamp"));
			resMap.put("bit", map.get("bit"));
			resMap.put("fps", map.get("fps"));
			resMap.put("standard_fps", map.containsKey("standard_fps") ? map.get("standard_fps") : "0");
			resMap.put("localaddr", map.get("localaddr"));
			resMap.put("address", map.get("address"));
			
			String flv_cs = "0";
			String flv_count = "0";
			if (onlineMap.containsKey(key)) {
				String value = onlineMap.get(key);
				flv_cs = value.split("######")[0];
				flv_count = value.split("######")[1];
			}
			resMap.put("flv_cs", flv_cs);
			resMap.put("flv_count", flv_count);
			
			String hls_cs = "0";
			String hls_count = "0";
			if (fcdHlsMap.containsKey(key)) {
				String value = fcdHlsMap.get(key);
				hls_cs = value.split("######")[0];
				hls_count = value.split("######")[1];
			}
			resMap.put("hls_cs", hls_cs);
			resMap.put("hls_count", hls_count);
			
			res.add(resMap);
		}
		return res;
	}

	@Override
	public void validate() {
		super.validate();
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
		}
		if (StringUtils.isBlank(domain_push)) {
			setMessage("fastmedia推流域名不能为空。");
		}
		if (StringUtils.isBlank(domain_play)) {
			setMessage("fastmedia播流域名不能为空。");
		}
		if (StringUtils.isBlank(domain_hls)) {
			setMessage("FCD播流域名不能为空。");
		}
		if (StringUtils.isBlank(stream)) {
			setMessage("流名称不能为空。");
		}
	/*	if (StringUtils.isBlank(app)) {
			setMessage("app名称不能为空。");
		}*/
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	public String getMessage() {
		return message;
	}

	public void setDomain_push(String domain_push) {
		this.domain_push = domain_push;
	}

	public void setDomain_play(String domain_play) {
		this.domain_play = domain_play;
	}

	public void setApp(String app) {
		this.app = app;
	}

	public void setStream(String stream) {
		this.stream = stream;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public void setPlatform(String platform) {
		this.platform = platform;
	}

	public void setDomain_hls(String domain_hls) {
		this.domain_hls = domain_hls;
	}
}
