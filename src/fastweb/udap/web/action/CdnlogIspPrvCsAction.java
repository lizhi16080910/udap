package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * 省份运营商 流量数据
 */
@Namespace("/")
@Action(value = "cdnlogispprvcs", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class CdnlogIspPrvCsAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private Map<String, Map<String, Long>> result_temp = new TreeMap<String, Map<String, Long>>();

	private List<Map<String, String>> result = new ArrayList<Map<String, String>>();

	public static final String INDEX = "isp.prv.cs";
	private String message;
	private String prv;
	private String isp;
	private long time = 0;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		if (initSpeed() == 0) {
			return ActionSupport.ERROR;
		}
		return ActionSupport.SUCCESS;
	}

	// 初始化慢速比数据
	public int initSpeed() {
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("time", this.time));

			AbstractAggregationBuilder cSum = AggregationBuilders.sum("traffic").field("traffic");
			AbstractAggregationBuilder prvKey = AggregationBuilders.terms("prv").field("prv").subAggregation(cSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder ispKey = AggregationBuilders.terms("isp").field("isp").subAggregation(prvKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(ispKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms ispAgge = response.getAggregations().get("isp");
			for (Bucket bucket : ispAgge.getBuckets()) {
				StringTerms prvAgge = bucket.getAggregations().get("prv");
				Map<String, Long> prvMap = new TreeMap<String, Long>();
				for (Bucket prvBucket : prvAgge.getBuckets()) {
					String key = prvBucket.getKey();
					Sum count = prvBucket.getAggregations().get("traffic");
					long value = ((Double) count.getValue()).longValue();
					prvMap.put(key, value);
				}
				result_temp.put(bucket.getKey(), prvMap);
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

	@Override
	public void validate() {
		super.validate();
		if (time == 0) {
			setMessage("查询时间time必填。");
		}
	}

	public List<Map<String, String>> parseResultTemp() {
		long sumCs = 0;
		List<Map<String, String>> lists_temp = new ArrayList<Map<String, String>>();
		List<Map<String, String>> lists = new ArrayList<Map<String, String>>();
		// 计算SumCs流量
		for (Entry<String, Map<String, Long>> ispEntrys : result_temp.entrySet()) {
			String isp = ispEntrys.getKey();
			Map<String, Long> PrvMap = ispEntrys.getValue();
			for (Entry<String, Long> entrys : PrvMap.entrySet()) {
				String prv = entrys.getKey();
				Long cs = entrys.getValue();
				Map<String, String> maps = new TreeMap<String, String>();
				maps.put("isp", isp);
				maps.put("prv", prv);
				maps.put("cs", String.valueOf(cs));
				lists_temp.add(maps);

				sumCs += cs;
			}
		}
		for (Map<String, String> map : lists_temp) {
			map.put("sumCs", String.valueOf(sumCs));
			lists.add(map);
		}
		return lists;
	}

	public List<Map<String, String>> getResult() {
		List<String> isps = null;
		List<String> prvs = null;
		List<Map<String, String>> lists = parseResultTemp();
		if (StringUtils.isNotBlank(isp)) {
			isps = Arrays.asList(isp.split(","));
			if (StringUtils.isNotBlank(prv)) {
				prvs = Arrays.asList(prv.split(","));
				for (Map<String, String> map : lists) {
					String isp_key = map.get("isp");
					String prv_key = map.get("prv");
					if (isps.contains(isp_key) && prvs.contains(prv_key)) {
						result.add(map);
					}
				}
			} else {
				for (Map<String, String> map : lists) {
					String isp_key = map.get("isp");
					if (isps.contains(isp_key)) {
						result.add(map);
					}
				}
			}
		} else {
			if (StringUtils.isNotBlank(prv)) {
				prvs = Arrays.asList(prv.split(","));
				for (Map<String, String> map : lists) {
					String prv_key = map.get("prv");
					if (prvs.contains(prv_key)) {
						result.add(map);
					}
				}
			} else {
				for (Map<String, String> map : lists) {
					result.add(map);
				}
			}
		}
		return result;
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

	public void setTime(long time) {
		this.time = time;
	}
}
