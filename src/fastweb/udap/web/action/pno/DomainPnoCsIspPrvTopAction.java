package fastweb.udap.web.action.pno;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
@Action(value = "domainpnocsispprvtop", results = {
		@Result(name = ActionSupport.SUCCESS, type = "json", params = {
				"excludeProperties", "message" }),
		@Result(name = ActionSupport.ERROR, type = "json", params = {
				"includeProperties", "message" }) })
public class DomainPnoCsIspPrvTopAction extends ActionSupport {
	public static final String INDEX = "pno_cs";
	private static final long serialVersionUID = 1L;
	/* www.hunantv.com:p1,p2;www.fastweb.com.cn:p1;www.163.com:p3,p4,p5 */
	private String domain;
	private String pno;
	private String isp;
	private String prv;
	private int top = 5 ;
	private long begin;
	private long end;
	private List<Map<String, Long>> result = new ArrayList<Map<String, Long>>();
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

				QueryBuilder ispQuery = QueryBuilders.termQuery("isp", isp);

				QueryBuilder prvQuery = QueryBuilders.termQuery("prv", prv);

				BoolQueryBuilder allQuery = QueryBuilders.boolQuery().must(
						timestampQuery).must(domainQuery).must(pnoQuery).must(
						ispQuery).must(prvQuery);

				SumBuilder csSum = AggregationBuilders.sum("cs").field("cs");

				TermsBuilder city = AggregationBuilders.terms("city").field(
						"city").size(top).subAggregation(csSum).order(
						Terms.Order.aggregation("cs", false));

				SearchResponse response = client.prepareSearch(INDEX).setQuery(
						allQuery).setFrom(0).setSize(0).addAggregation(city)
						.execute().actionGet();

				StringTerms cityTerms = response.getAggregations().get("city");

				for (Bucket cityBucket : cityTerms.getBuckets()) {
					Map<String, Long> cityAndCs = new HashMap<String, Long>();
					String cityKey = cityBucket.getKey();
					Sum sum = cityBucket.getAggregations().get("cs");
					Long cs = ((Double) sum.getValue()).longValue();
					cityAndCs.put(cityKey, cs);
					result.add(cityAndCs);
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
		if (StringUtil.isEmpty(isp)) {
			setMessage("参数isp为空");
		}
		if (StringUtil.isEmpty(prv)) {
			setMessage("参数prv为空");
		}
		if (begin > end) {
			setMessage("开始时间戳大于结束时间戳");
		}
	}

	public List<Map<String, Long>> getResult() {
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

	public void setIsp(String isp) {
		this.isp = isp;
	}

	public void setPrv(String prv) {
		this.prv = prv;
	}

	public void setTop(int top) {
		this.top = top;
	}
}
