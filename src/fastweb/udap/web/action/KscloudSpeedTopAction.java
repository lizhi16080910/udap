package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * 各运营商用户OR设备IP的top20慢速次数Action
 */
@Namespace("/")
@Action(value = "kscloudspeedtop", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class KscloudSpeedTopAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	private Map<String, Long> result = new TreeMap<String, Long>();
	public static final String INDEX = "kscloud_speed";
	private String message;
	private String domain;
	private String prv;
	private String isp;
	private String userid;
	private String ipstatus;
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
			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
			// 慢速比<80(kb/s)
			QueryBuilder ratioQuery = QueryBuilders.termQuery("ratio", "0");
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery).must(ratioQuery);
			// all查询所有isp数据，参数不为all，查询对应的isp
			if (!"all".equalsIgnoreCase(isp)) {
				if ("ip".equals(ipstatus)) {
					// 用户ip，isp使用用户的isp
					QueryBuilder ispQuery = QueryBuilders.termQuery("isp", this.isp);
					allQuery.must(ispQuery);
				} else {
					// 设备ip，isp使用设备的serverisp
					QueryBuilder ispQuery = QueryBuilders.termQuery("serverisp", this.isp);
					allQuery.must(ispQuery);
				}
			}
			// userid和domain都不传，就是查询ES里面的所有域名的情况，不userid匹配
			if ("all".equalsIgnoreCase(domain) && userid != null) {
				// 客户userid查询
				allQuery = allQuery.must(QueryBuilders.termQuery("userid", this.userid));
			} else if (domain != null && !domain.equalsIgnoreCase("all") && userid == null) {
				// 单域名查询
				allQuery = allQuery.must(QueryBuilders.termQuery("domain", this.domain));
			}

			if (StringUtils.isNotBlank(prv)) {
				QueryBuilder prvQuery = QueryBuilders.termQuery("prv", this.prv);
				allQuery.must(prvQuery);
			}
			AbstractAggregationBuilder cSum = AggregationBuilders.sum("count").field("count");
			// 按照分组后的count排序，取前20
			AbstractAggregationBuilder ipBuilder = AggregationBuilders.terms(ipstatus).field(ipstatus).subAggregation(cSum).size(1000).order(Terms.Order.aggregation("count", false));

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(ipBuilder);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			LongTerms ipAgge = response.getAggregations().get(ipstatus);
			for (Bucket ipBucket : ipAgge.getBuckets()) {
				String ipKey = ipBucket.getKey();
				Sum count = ipBucket.getAggregations().get("count");
				long value = ((Double) count.getValue()).longValue();
				result.put(ipKey, value);
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

	public static Map<String, Long> sortMapByValue(Map<String, Long> map) {
		Map<String, Long> sortedMap = new LinkedHashMap<String, Long>();
		if (map == null || map.isEmpty()) {
			return sortedMap;
		}
		List<Map.Entry<String, Long>> entryList = new ArrayList<Map.Entry<String, Long>>(map.entrySet());
		Collections.sort(entryList, new MapValueComparator());
		Iterator<Map.Entry<String, Long>> iter = entryList.iterator();
		Map.Entry<String, Long> tmpEntry = null;
		int i = 0;
		while (iter.hasNext() && i < 20) {
			tmpEntry = iter.next();
			sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());
			i++;
		}
		return sortedMap;
	}

	@Override
	public void validate() {
		super.validate();
		if (!"ip".equals(ipstatus) && !"serverip".equals(ipstatus)) {
			setMessage("ipstatus不能为空，且正确类型。");
		}
		if (StringUtil.isEmpty(isp)) {
			setMessage("运营商不能为空。");
		}
		if (begin > end) {
			setMessage("开始时间必须小于结束时间。");
		}
	}

	public Map<String, Long> getResult() {
		return sortMapByValue(result);
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

	public void setUserid(String userid) {
		this.userid = userid;
	}

	public void setIpstatus(String ipstatus) {
		this.ipstatus = ipstatus;
	}
}

// 比较器类
class MapValueComparator implements Comparator<Map.Entry<String, Long>> {
	public int compare(Entry<String, Long> me1, Entry<String, Long> me2) {
		return me2.getValue().compareTo(me1.getValue());
	}
}
