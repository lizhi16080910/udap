package fastweb.udap.web.action.pno;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.metrics.sum.Sum;
import org.elasticsearch.search.aggregations.metrics.sum.SumBuilder;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.StringUtil;
import fastweb.udap.web.EsClientFactory;

/**
 * 
 * @author shuzhangyao
 * @date 2015-09-15 16：15
 *       <p>
 *       hntv流量地域查询功能
 *       </p>
 * 
 */
@Namespace("/")
@Action(value = "domainpnocstop", results = {
		@Result(name = ActionSupport.SUCCESS, type = "json", params = {
				"excludeProperties", "message" }),
		@Result(name = ActionSupport.ERROR, type = "json", params = {
				"includeProperties", "message" }) })
public class DomainPnoCsTopAction extends ActionSupport {
	public static final String INDEX = "pno_cs";
	private static final long serialVersionUID = 1L;
	/* www.hunantv.com:p1,p2;www.fastweb.com.cn:p1;www.163.com:p3,p4,p5 */
	private String domain;
	private String pno;
	private long begin;
	private long end;
	private int top = 5;
	private Map<String, List<Map<String, Long>>> result = new HashMap<String, List<Map<String, Long>>>();
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
						domain);

				QueryBuilder pnoQuery = QueryBuilders.termQuery("pno", pno);

				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(
						timestampQuery).must(domainQuery).must(pnoQuery);

				SumBuilder csSum = AggregationBuilders.sum("cs").field("cs");

				TermsBuilder prv = AggregationBuilders.terms("prv")
						.field("prv").size(top).subAggregation(csSum).order(
								Terms.Order.aggregation("cs", false));

				TermsBuilder isp = AggregationBuilders.terms("isp")
						.field("isp").size(Integer.MAX_VALUE).subAggregation(
								prv);

				SearchResponse response = client.prepareSearch(INDEX).setQuery(
						allQuery).setFrom(0).setSize(0).addAggregation(isp)
						.execute().actionGet();

				StringTerms ispTerms = response.getAggregations().get("isp");

				for (Bucket ispBucket : ispTerms.getBuckets()) {
					String ispKey = ispBucket.getKey();
					StringTerms prvTerms = ispBucket.getAggregations().get(
							"prv");
					List<Map<String, Long>> prvs = new ArrayList<Map<String, Long>>();
					for (Bucket prvBucket : prvTerms.getBuckets()) {
						Map<String, Long> prvAndCs = new HashMap<String, Long>();
						String prvKey = prvBucket.getKey();
						Sum sum = prvBucket.getAggregations().get("cs");
						Long cs = ((Double) sum.getValue()).longValue();
						prvAndCs.put(prvKey, cs);
						prvs.add(prvAndCs);
					}
					result.put(ispKey, prvs);
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
		if (StringUtil.isEmpty(pno)) {
			setMessage("参数pno为空");
		}
		if (begin > end) {
			setMessage("开始时间戳大于结束时间戳");
		}
	}

	public Map<String, List<Map<String, Long>>> getResult() {
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

	public void setPno(String pno) {
		this.pno = pno;
	}

	public void setTop(int top) {
		this.top = top;
	}
}
