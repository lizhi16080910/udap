package fastweb.udap.web.action.pno;

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
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

@Namespace("/domainpnocs")
@Action(value = "year", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "message" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties", "message" }) })
public class DomainPnoCsAction extends ActionSupport {
	/**
	 * 湖南卫视pno数据需要保存一年，没5分钟聚合生成一个点。
	 */
	public static final String INDEX = "pno_off_five";
	private static final long serialVersionUID = 1L;
	/*www.hunantv.com:p1,p2;www.fastweb.com.cn:p1;www.163.com:p3,p4,p5*/
	private String domainAndPnos;
	private long begin;
	private long end;
	private Map<String, Map<String,Map<Long,Long>>> result_temp = new TreeMap<String, Map<String,Map<Long,Long>>>();
	private Map<String, Map<String,Map<Long,Long>>> result = new TreeMap<String, Map<String,Map<Long,Long>>>();
	private String message;

	@Override
	public String execute() throws Exception {
		if(!StringUtils.isBlank(message)){
			return ActionSupport.ERROR;
		}
		try {
			Client client = EsClientFactory.createClient();
			try {
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
					if(domainPnos.length==2){
						pnos = domainPnos[1].split(",");
						TermsQueryBuilder pnosQuery = QueryBuilders.termsQuery("pno", pnos);
						boolQueryBuilder.must(pnosQuery);
					}
					domAndPnoQuery.should(boolQueryBuilder);
				}
				/*两个must，	第一个：时间戳，	第二个：多个domain和pnos组成的or*/
				allQuery.must(domAndPnoQuery);

				AbstractAggregationBuilder csSum = AggregationBuilders.sum("cs").field("cs");
				AbstractAggregationBuilder timestampKey = AggregationBuilders.terms("timestamp").field("timestamp").subAggregation(csSum).size(Integer.MAX_VALUE);
				AbstractAggregationBuilder pnoKey = AggregationBuilders.terms("pno").field("pno").subAggregation(timestampKey).size(Integer.MAX_VALUE);
				AbstractAggregationBuilder domainKey = AggregationBuilders.terms("domain").field("domain").subAggregation(pnoKey).size(Integer.MAX_VALUE);
				SearchResponse response = client.prepareSearch(INDEX).setQuery(allQuery).setFrom(0).setSize(0).addAggregation(domainKey).execute().actionGet();
				
//				System.out.println(response);
				
				StringTerms domainTerms = response.getAggregations().get("domain");
				for(Bucket domainBucket : domainTerms.getBuckets()){
					Map<String,Map<Long,Long>> pnocs = new TreeMap<String,Map<Long,Long>>();
					String domain = domainBucket.getKey();
					StringTerms pnoTerms = domainBucket.getAggregations().get("pno");
					for(Bucket pnoBucket : pnoTerms.getBuckets()){
						String pno = pnoBucket.getKey();
						StringTerms agge = pnoBucket.getAggregations().get("timestamp");
						Map<Long,Long> timeMap = new TreeMap<Long,Long>();
						for (Bucket bucket : agge.getBuckets()) {
							Sum sum = bucket.getAggregations().get("cs");
							long key = Long.parseLong(bucket.getKey());
                            //ES中的数据已向前聚合，此处不用在向前聚合。
							//long newKey = key / 300 * 300+300;
							long newKey = key;
							long value = ((Double) sum.getValue()).longValue();
							if (timeMap.containsKey(newKey)) {
								long newValue = timeMap.get(newKey) + value;
								timeMap.put(newKey, newValue);
							} else {
								timeMap.put(newKey, value);
							}
						}
						pnocs.put(pno, timeMap);
					}
					result_temp.put(domain,pnocs);
				}
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
		if(StringUtil.isEmpty(domainAndPnos)){
			setMessage("参数domain为空");
		}
		if(begin > end){
			setMessage("开始时间戳大于结束时间戳");
		}
	}
	
	public Map<String, Map<String, Map<Long,Long>>> getResult() {
		String[] domainPnoPairs = domainAndPnos.split(";");
		for(String domainPnoPair : domainPnoPairs){
			String[] domainPnos = domainPnoPair.split(":");
			String domain = domainPnos[0];
			String pnos = null;
			if(domainPnos.length==2){
				pnos = domainPnos[1];
			}
			if(result_temp.containsKey(domain)) {
				Map<String, Map<Long,Long>> domainMap = result_temp.get(domain);
				String[] pnoArray = pnos.split(",");
				for(String pno : pnoArray){
					if(!domainMap.containsKey(pno)){
						Map<Long,Long> timeMap = new TreeMap<Long,Long>();
						domainMap.put(pno,timeMap);
					}
				}
				result.put(domain, domainMap);
			} else {
				Map<String, Map<Long,Long>> domainMap = new TreeMap<String, Map<Long,Long>>();
				if(domainPnos.length==2){
					String[] pnoArray = pnos.split(",");
					for(String pno : pnoArray){
						Map<Long,Long> timeMap = new TreeMap<Long,Long>();
						domainMap.put(pno, timeMap);
					}
				}
				result.put(domain, domainMap);
			}
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
