package fastweb.udap.web.glb;

import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import net.sf.json.JSONObject;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.metrics.avg.Avg;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;



public class CdnlogGlbSpeed {
	public static final String INDEX = "cdnlog.isp.prv.node.join.speed";
	private static Map<String, Object> result = new TreeMap<String, Object>();

	public static void main(String[] args) {
		// 开始时间
		long begin = Long.valueOf(args[0]);

		// 结束时间
		long end = Long.valueOf(args[1]);
		
		// 生成文件   "/var/www/html/fastweb/bigdata/udap/glb/latest/latest.glb"
		String Path = args[2];

		try {
			Client client = EsClientFactory.createClient();
			try {
				QueryBuilder timestampQuery = QueryBuilders
						.rangeQuery("timestamp").from(begin).to(end);

				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(
						timestampQuery);

				AbstractAggregationBuilder countSum = AggregationBuilders.sum(
						"ct").field("count");
				AbstractAggregationBuilder csSum = AggregationBuilders
						.sum("cs").field("cs");
				AbstractAggregationBuilder esSum = AggregationBuilders
						.sum("es").field("es");
				AbstractAggregationBuilder medianAvg = AggregationBuilders.avg(
						"md").field("speed");
				String[] termBucketsName = { "platform", "isp", "prv", "node",
						"timestamp" };

				TermsBuilder domainAgg = AggregationBuilders.terms("userid")
						.field("userid").size(Integer.MAX_VALUE)
						.subAggregation(countSum).subAggregation(csSum)
						.subAggregation(esSum).subAggregation(medianAvg);
				for (String bucketName : termBucketsName) {
					TermsBuilder termAgg = AggregationBuilders
							.terms(bucketName).field(bucketName);
					termAgg.subAggregation(domainAgg).size(Integer.MAX_VALUE);
					domainAgg = termAgg;
				}

				SearchResponse response = client.prepareSearch(INDEX)
						.setQuery(allQuery).setFrom(0).setSize(0)
						.addAggregation(domainAgg).execute().actionGet();

				result = genResult(response);
				
				
				JSONObject jsonObject = JSONObject.fromObject(result);

				File f = new File(Path);
				FileWriter os = new FileWriter(f);
				os.write(jsonObject.toString());
				os.close();

			} finally {
				client.close();
			}
		} catch (Exception e) {
		}

	}


	public static Map<String, Object> genResult(SearchResponse response) {
		Map<String, Object> resultMap = new TreeMap<String, Object>();
		List<Aggregation> aggList = response.getAggregations().asList();
		if (!aggList.isEmpty()) {
			for (Aggregation agg : aggList) {
				resultMap.put(agg.getName(), getAggResult(agg));
			}
		}
		return resultMap;
	}

	public static Object getAggResult(Aggregation agg) {

		if (agg instanceof Terms) { /* 非 leaf aggregation ,此时返回结果中不需要该agg的名字 */
			List<Bucket> bucketList = ((Terms) agg).getBuckets();
			Map<String, Object> bucketMap = new HashMap<String, Object>();
			for (Bucket buck : bucketList) {
				bucketMap.put(buck.getKey(), getBuckResult(buck));
			}
			return bucketMap;
		} else if (agg instanceof Sum || agg instanceof Avg) { /*
																 * leaf
																 * Aggregation
																 */
			if (agg.getName().equalsIgnoreCase("es")) {
				return (float) ((Sum) agg).getValue();
			}
			if (agg.getName().equalsIgnoreCase("md")) {
				return (float) ((Avg) agg).getValue();
			}
			Long sumValue = ((Double) ((Sum) agg).getValue()).longValue();
			return sumValue;
		} else {
			return null;
		}
	}

	private static Object getBuckResult(Bucket buck) { /* buck下一定有数据 */

		List<Aggregation> aggList = buck.getAggregations().asList();

		if(aggList.size() > 1) {
			Map<String,Object> buckRst = new TreeMap<String,Object>();
			for( Aggregation agg : aggList) {
				buckRst.put(agg.getName(),getAggResult(agg));
			}
			return buckRst;
		} else {
			return getAggResult(aggList.get(0)); /* 只有一个子aggregation */
		}

	}
		
}

