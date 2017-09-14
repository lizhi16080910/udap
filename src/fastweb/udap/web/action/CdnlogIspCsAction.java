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
 * 运营商 流量数据
 */
@Namespace("/")
@Action(value = "cdnlogispcs", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class CdnlogIspCsAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private Map<String, Long> result_temp = new TreeMap<String, Long>();

	private List<Map<String, String>> result = new ArrayList<Map<String, String>>();

	public static final String INDEX = "isp.prv.cs";
	private String message;
	private String ispORprv;
	private String type;

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
			AbstractAggregationBuilder typeKey = AggregationBuilders.terms(type).field(type).subAggregation(cSum).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(typeKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms typeAgge = response.getAggregations().get(type);
			for (Bucket bucket : typeAgge.getBuckets()) {
				String key = bucket.getKey();
				Sum count = bucket.getAggregations().get("traffic");
				long value = ((Double) count.getValue()).longValue();
				result_temp.put(key, value);
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
		if(StringUtils.isBlank(type)){
			setMessage("查询类型type不能为空.");
		}
		if (StringUtils.isNotBlank(type) && (!type.equals("isp")) && (!type.equals("prv"))) {
			setMessage("查询类型type必须是(isp和prv)两者中的一种.");
		}
	}

	public long parseResultTemp() {
		long sumCs = 0;
		// 计算SumCs流量
		for (Entry<String, Long> typeEntrys : result_temp.entrySet()) {
			Long cs = typeEntrys.getValue();
			sumCs += cs;
		}
		return sumCs;
	}

	public List<Map<String, String>> getResult() {
		long sumCs = parseResultTemp();
		List<String> types = null;
		if (StringUtils.isNotBlank(ispORprv)) {
			types = Arrays.asList(ispORprv.split(","));
			for (Entry<String, Long> typeEntrys : result_temp.entrySet()) {
				String type_key = typeEntrys.getKey();
				Long value = typeEntrys.getValue();
				if (types.contains(type_key)) {
					Map<String, String> maps = new TreeMap<String, String>();
					maps.put(type, type_key);
					maps.put("cs", String.valueOf(value));
					maps.put("sumCs", String.valueOf(sumCs));
					result.add(maps);
				}
			}
		} else {
			for (Entry<String, Long> typeEntrys : result_temp.entrySet()) {
				String type_key = typeEntrys.getKey();
				Long value = typeEntrys.getValue();
				Map<String, String> maps = new TreeMap<String, String>();
				maps.put(type, type_key);
				maps.put("cs", String.valueOf(value));
				maps.put("sumCs", String.valueOf(sumCs));
				result.add(maps);
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

	public void setIspORprv(String ispORprv) {
		this.ispORprv = ispORprv;
	}

	public void setType(String type) {
		this.type = type;
	}

	public void setTime(long time) {
		this.time = time;
	}
}
