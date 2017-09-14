package fastweb.udap.web.action;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.xwork.StringUtils;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

@Namespace("/")
@Action(value = "domaindircs", results = {
		@Result(name = ActionSupport.SUCCESS, type = "json", params = {
				"excludeProperties", "message" }),
		@Result(name = ActionSupport.ERROR, type = "json", params = {
				"includeProperties", "message" }) })
public class DomainDirCsAction extends ActionSupport {
	public static final String INDEX = "dir_cs";
	private static final long serialVersionUID = 1L;
	private String domain;
	private long begin;
	private long end;
	private Map<String, Map<Long, Long>> result = new TreeMap<String, Map<Long, Long>>();
	private String message;

	@Override
	public String execute() throws Exception {
		if (!StringUtils.isBlank(message)) {
			return ActionSupport.ERROR;
		}
		try {
			Client client = EsClientFactory.createClient();
			try {
				QueryBuilder timestampQuery = QueryBuilders.rangeQuery(
						"timestamp").from(begin).to(end);

				QueryBuilder domainQuery = QueryBuilders.termQuery("domain",
						this.domain);

				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(
						timestampQuery).must(domainQuery);

				AbstractAggregationBuilder csSum = AggregationBuilders
						.sum("cs").field("cs");

				AbstractAggregationBuilder timestampKey = AggregationBuilders
						.terms("timestamp").field("timestamp").subAggregation(
								csSum).size(Integer.MAX_VALUE);

				AbstractAggregationBuilder dir1Key = AggregationBuilders.terms(
						"dir1").field("dir1").subAggregation(timestampKey)
						.size(Integer.MAX_VALUE);

				SearchResponse response = client.prepareSearch(INDEX).setQuery(
						allQuery).setFrom(0).setSize(0).addAggregation(dir1Key)
						.execute().actionGet();


				StringTerms dir1Terms = response.getAggregations().get("dir1");
				for (Bucket dir1Bucket : dir1Terms.getBuckets()) {
					String dir1 = dir1Bucket.getKey();
					LongTerms agge = dir1Bucket.getAggregations().get(
							"timestamp");
					Map<Long, Long> timestampMap = new TreeMap<Long, Long>();
					for (Bucket timestampBucket : agge.getBuckets()) {
						Sum sum = timestampBucket.getAggregations().get("cs");
						Long key = Long.parseLong(timestampBucket.getKey());
						Long newKey = key / 300 * 300+300;
						long value = ((Double) sum.getValue()).longValue();
						if (timestampMap.containsKey(newKey)) {
							long newValue = timestampMap.get(newKey) + value;
							timestampMap.put(newKey, newValue);
						} else {
							timestampMap.put(newKey, value);
						}
					}
					result.put(dir1, timestampMap);
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
			setMessage("参数domain为空");
		}
		if (begin > end) {
			setMessage("开始时间戳大于结束时间戳");
		}
	}

	public Map<String, Map<Long, Long>> getResult() {
		return result;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
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
}
