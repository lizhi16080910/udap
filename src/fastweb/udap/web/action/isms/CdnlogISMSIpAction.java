package fastweb.udap.web.action.isms;

import java.text.NumberFormat;
import java.util.ArrayList;
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
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.max.Max;
import org.elasticsearch.search.aggregations.metrics.min.Min;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.DatetimeUtil;
import fastweb.udap.util.IpListUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * isms按设备ip分组，查询条数count 和 首次访问时间（CDN节点IP统计查询）
 */
@Namespace("/")
@Action(value = "cdnlogismsip", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class CdnlogISMSIpAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "cdnlog.isms.ip";
	public static final String INDEX_HOUR = "cdnlog.isms.ip.hour";

	/* 结束时间戳 */
	private long time = 0;
	/* 开始时间戳 */
	private long begin = 0;
	/* 节点IP */
	private String nodeip;

	private String message;

	/* 每页显示条数 */
	private int size = 100;
	/* 开始的记录数 */
	private int from = 0;

	private long total = 0;

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

	@Override
	public String execute() throws Exception {
		NumberFormat nf = NumberFormat.getInstance();
        nf.setGroupingUsed(false);
		
		long queryStartTime = 0;
		long queryEndTime = 0;
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		if (begin == 0) {
			long ct = System.currentTimeMillis() / 1000;
			String currentTime = DatetimeUtil.timestampToDateStr(System.currentTimeMillis() / 1000, "yyyyMMdd");
			String queryTime = DatetimeUtil.timestampToDateStr(time, "yyyyMMdd");
			if (currentTime.equals(queryTime)) {
				queryEndTime = ct;
				queryStartTime = ct / 86400 * 86400 - 8 * 3600;
			} else {
				// 当天23点59分59秒
				queryEndTime = time + 24 * 3600 - 1;
				queryStartTime = time;
			}
		} else {
			queryStartTime = begin;
			queryEndTime = time;
		}

		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {

			client = EsClientFactory.createClient();

			QueryBuilder timeQuery = QueryBuilders.rangeQuery("time").from(queryStartTime).to(queryEndTime);

			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timeQuery);

			if (StringUtils.isNotBlank(nodeip)) {
				QueryBuilder nodeipQuery = QueryBuilders.termQuery("ip", IpListUtil.ip2Long(nodeip));
				allQuery.must(nodeipQuery);
			}
			
			AbstractAggregationBuilder cSum = AggregationBuilders.sum("count").field("count");
			AbstractAggregationBuilder min = AggregationBuilders.min("min_startTime").field("startTime");
			AbstractAggregationBuilder max = AggregationBuilders.max("max_endTime").field("endTime");
			AbstractAggregationBuilder ipKey = AggregationBuilders.terms("ip").field("ip").subAggregation(cSum).subAggregation(min).subAggregation(max).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX_HOUR).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(ipKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());
			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms ipAgge = response.getAggregations().get("ip");

			for (Bucket ipbucket : ipAgge.getBuckets()) {
				Map<String, Object> maps = new TreeMap<String, Object>();
				String ip = ipbucket.getKey();
				
				Sum count = ipbucket.getAggregations().get("count");
				Max max_value = ipbucket.getAggregations().get("max_endTime");
				Min min_value = ipbucket.getAggregations().get("min_startTime");

				long value = ((Double) count.getValue()).longValue();
				String maxValue = nf.format(max_value.getValue());
				String minValue = nf.format(min_value.getValue());
				
				maps.put("startTime", DatetimeUtil.parseTime(minValue));
				maps.put("endTime", DatetimeUtil.parseTime(maxValue));
				maps.put("ip", IpListUtil.ipToString(Long.valueOf(ip)));
				maps.put("count", value);
				result.add(maps);
			}
			this.setTotal(result.size());
			/*
			SearchResponse response = searchRequestBuilder.execute().actionGet();
			Iterator<SearchHit> it = response.getHits().iterator();

			this.setTotal(response.getHits().totalHits());

			while (it.hasNext()) {
				SearchHit sh = it.next();
				Map<String, Object> source = sh.getSource();
				try {
					String startTime = String.valueOf(source.get("startTime"));
					String endTime = String.valueOf(source.get("endTime"));
					String ip = String.valueOf(source.get("ip"));
					source.put("startTime", DatetimeUtil.parseTime(startTime));
					source.put("endTime", DatetimeUtil.parseTime(endTime));
					source.put("ip", IpListUtil.ipToString(Long.valueOf(ip)));
					result.add(source);
				} catch (Exception e) {
					continue;
				}
			}
			*/
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
		if (time == 0) {
			setMessage("时间戳不能为空。");
		}
	}

	public List<Map<String, Object>> getResult() {
		return result;
	}

	public void setNodeip(String nodeip) {
		this.nodeip = nodeip;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setSize(int size) {
		this.size = size;
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
