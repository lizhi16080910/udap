package fastweb.udap.web.action;

import java.util.Map;
import java.util.TreeMap;

import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

@Namespace("/")
@Action(value = "dircs", results = {
		@Result(name = ActionSupport.SUCCESS, type = "json", params = {
				"excludeProperties", "message" }),
		@Result(name = ActionSupport.ERROR, type = "json", params = {
				"includeProperties", "message" }) })
public class DirCsAction extends ActionSupport {
	public static final String INDEX = "dir_cs";
	private static final long serialVersionUID = 1L;
	private String domain;
	private String dir1;
	private String dir2;
	private long begin;
	private long end;
	private Map<Long, Long> result = new TreeMap<Long, Long>();
	private String message;

	/**
	 * 
	 */

	@Override
	public String execute() throws Exception {
		try {
			Client client = EsClientFactory.createClient();
			try {
				QueryBuilder domainQuery = QueryBuilders.termQuery("domain",
						this.domain);

				QueryBuilder dir1Query = QueryBuilders.termQuery("dir1",
						this.dir1);

				QueryBuilder dir2Query = QueryBuilders.termQuery("dir2",
						this.dir2);

				QueryBuilder timestampQuery = QueryBuilders.rangeQuery(
						"timestamp").from(begin).to(end);

				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(
						domainQuery).must(timestampQuery);
				if (this.dir1 != null && !this.dir1.equals("")) {
					allQuery.must(dir1Query);
				}
				if (this.dir2 != null && !this.dir2.equals("")) {
					allQuery.must(dir2Query);
				}

				AbstractAggregationBuilder csSum = AggregationBuilders
						.sum("cs").field("cs");
				AbstractAggregationBuilder timestampKey = AggregationBuilders
						.terms("timestamp").field("timestamp").subAggregation(
								csSum).size(Integer.MAX_VALUE);
				SearchResponse response = client.prepareSearch(INDEX).setQuery(
						allQuery).setFrom(0).setSize(0).addAggregation(
						timestampKey).execute().actionGet();
				LongTerms agge = response.getAggregations().get("timestamp");
				for (Bucket bucket : agge.getBuckets()) {
					Sum sum = bucket.getAggregations().get("cs");
					long key = Long.parseLong(bucket.getKey());

					long newKey = key / 300 * 300+300;
					long value = ((Double) sum.getValue()).longValue();
					if (result.containsKey(newKey)) {
						long newValue = result.get(newKey) + value;
						result.put(newKey, newValue);
					} else {
						result.put(newKey, value);
					}
				}
			} finally {
				client.close();
			}
		} catch (Exception e) {
			this.message = "请求错误";
			return ActionSupport.ERROR;
		}

		return ActionSupport.SUCCESS;
	}

	public Map<Long, Long> getResult() {
		return result;
	}

	public String getMessage() {
		return message;
	}

	public void setDomain(String domain) {
		this.domain = domain;
	}

	public void setDir1(String dir1) {
		this.dir1 = dir1;
	}

	public void setDir2(String dir2) {
		this.dir2 = dir2;
	}

	public void setBegin(long begin) {
		this.begin = begin;
	}

	public void setEnd(long end) {
		this.end = end;
	}

}
