package test;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.LongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.junit.Test;

import fastweb.udap.web.EsClientFactory;

public class DirCsTest {
	private String domain = "seg2.videocc.net";
	private String begin = "1433923320";
	private String end = "1433926980";
	private String INDEX = "dir_cs";

	@Test
	public void getData() {
		try {
			Client client = EsClientFactory.createClient();
			try {
				QueryBuilder domainQuery = QueryBuilders.termQuery("domain",
						domain);

				QueryBuilder dir1Query = QueryBuilders.termQuery("dir1", "pts");

				// QueryBuilder dir2Query = QueryBuilders.termQuery("dir2",
				// this.dir2);

				QueryBuilder timestampQuery = QueryBuilders.rangeQuery(
						"timestamp").from(begin).to(end);

				QueryBuilder allQuery = QueryBuilders.boolQuery().must(
						domainQuery).must(timestampQuery);// .must(dir1Query);

				AbstractAggregationBuilder csSum = AggregationBuilders.sum(
						"sum_cs").field("cs");

				AbstractAggregationBuilder total = AggregationBuilders.terms(
						"timestamp").field("timestamp").size(Integer.MAX_VALUE).subAggregation(csSum);
				SearchResponse response = client.prepareSearch(INDEX).setQuery(
						allQuery).setFrom(0).setSize(0).addAggregation(total)
						.execute().actionGet();

				// Iterator<SearchHit> it = response.getHits().iterator();

				LongTerms agge = response.getAggregations().get("timestamp");
				for (Bucket bucket : agge.getBuckets()) {
					Sum sum = bucket.getAggregations().get("sum_cs");
					System.out.println("key :"+bucket.getKey()+"\t"+new Double(sum.getValue()).longValue());
				}

				System.out.println(response);
				System.out.println(agge);
				System.out.println(System.currentTimeMillis()/1000);
			} finally {
				client.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
