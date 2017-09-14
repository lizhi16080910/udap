package fastweb.udap.web.action;

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
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

@Namespace("/")
@Action(value = "domainpnocs", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class NewDomainPnoCsAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	private final Log log = LogFactory.getLog(getClass());

	public static final String INDEX = "pnocs";
	/* www.hunantv.com:p1,p2;www.fastweb.com.cn:p1;www.163.com:p3,p4,p5 */
	private String domainAndPnos;
	private long begin;
	private long end;
	private Map<String, Map<String, Map<String, Long>>> result_temp = new TreeMap<String, Map<String, Map<String, Long>>>();
	private Map<String, Map<String, Map<String, Long>>> result = new TreeMap<String, Map<String, Map<String, Long>>>();
	private String message;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			log.warn("params error: " + message + begin + "\t" + end + "\t" + domainAndPnos);
			return ActionSupport.ERROR;
		}
		Client client = null;
		SearchRequestBuilder searchRequestBuilder = null;
		try {
			client = EsClientFactory.createClient();

			QueryBuilder timestampQuery = QueryBuilders.rangeQuery("timestamp").from(begin).to(end);
			BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(timestampQuery);
			String[] domainPnoPairs = domainAndPnos.split(";");
			BoolQueryBuilder domAndPnoQuery = QueryBuilders.boolQuery();
			for (String domainPnoPair : domainPnoPairs) {
				String[] domainPnos = domainPnoPair.split(":");
				String domain = null;
				String[] pnos = null;
				domain = domainPnos[0];
				TermQueryBuilder termQuery = QueryBuilders.termQuery("domain", domain);
				BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(termQuery);
				if (domainPnos.length == 2) {
					pnos = domainPnos[1].split(",");
					TermsQueryBuilder pnosQuery = QueryBuilders.termsQuery("pno", pnos);
					boolQueryBuilder.must(pnosQuery);
				}
				domAndPnoQuery.should(boolQueryBuilder);
			}
			allQuery.must(domAndPnoQuery);

			AbstractAggregationBuilder csSum = AggregationBuilders.sum("cs").field("cs");
			AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("fivetime").field("fivetime").subAggregation(csSum).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder pnoKey = AggregationBuilders.terms("pno").field("pno").subAggregation(timestampKey).size(Integer.MAX_VALUE);
			AbstractAggregationBuilder domainKey = AggregationBuilders.terms("domain").field("domain").subAggregation(pnoKey).size(Integer.MAX_VALUE);

			searchRequestBuilder = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(domainKey);
			log.info("Query Url: " + ServletActionContext.getRequest().getRemoteAddr() + ": " + searchRequestBuilder.toString());

			SearchResponse response = searchRequestBuilder.execute().actionGet();

			StringTerms domainTerms = response.getAggregations().get("domain");
			for (Bucket domainBucket : domainTerms.getBuckets()) {
				Map<String, Map<String, Long>> pnocs = new TreeMap<String, Map<String, Long>>();
				String domain = domainBucket.getKey();
				StringTerms pnoTerms = domainBucket.getAggregations().get("pno");
				for (Bucket pnoBucket : pnoTerms.getBuckets()) {
					String pno = pnoBucket.getKey();
					StringTerms agge = pnoBucket.getAggregations().get("fivetime");
					Map<String, Long> timeMap = new TreeMap<String, Long>();
					for (Bucket bucket : agge.getBuckets()) {
						String key = bucket.getKey();
						Sum sum = bucket.getAggregations().get("cs");
						long value = ((Double) sum.getValue()).longValue();
						timeMap.put(key, value);
					}
					pnocs.put(pno, timeMap);
				}
				result_temp.put(domain, pnocs);
			}
		} catch (RuntimeException e) {
			log.error("Query Fail: " + e);
			this.message = "请求失败";
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
		if (StringUtil.isEmpty(domainAndPnos)) {
			setMessage("参数domain为空");
		}
		if (begin > end) {
			setMessage("开始时间戳大于结束时间戳");
		}
	}

	public Map<String, Map<String, Map<String, Long>>> getResult() {
		String[] domainPnoPairs = domainAndPnos.split(";");
		for (String domainPnoPair : domainPnoPairs) {
			String[] domainPnos = domainPnoPair.split(":");
			String domain = domainPnos[0];
			Map<String, Map<String, Long>> domainMap = new TreeMap<String, Map<String, Long>>();
			if (result_temp.containsKey(domain)) {
				domainMap = result_temp.get(domain);
			}
			if (domainPnos.length == 2) {
				String[] pnoArray = domainPnos[1].split(",");
				for (String pno : pnoArray) {
					if (!domainMap.containsKey(pno)) {
						Map<String, Long> timeMap = new TreeMap<String, Long>();
						domainMap.put(pno, timeMap);
					}
				}
			}
			result.put(domain, domainMap);
		}
		return result;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public void setDomainAndPnos(String domainAndPnos) {
		this.domainAndPnos = domainAndPnos;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}
}