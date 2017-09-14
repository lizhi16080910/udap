package fastweb.udap.web.action;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.web.EsClientFactory;

/**
 * 
 * @author shuzhangyao
 * 
 */
@Namespace("/")
@Action(value = "nohitmachine", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {}) })
public class NohitMachineAction extends ActionSupport {
	private static final long serialVersionUID = 1L;

	public static final String INDEX = "nohit.machine.count";

	public static final String BACK_PARENT_HIT_MACHINE_ONE = "back.parent.hit.machine.one";

	public static final String BACK_PARENT_HIT_MACHINE_FIVE = "back.parent.hit.machine.five";

	public static final String BACK_PARENT_HIT_MACHINE_HOUR = "back.parent.hit.machine.hour";

	public static final String BACK_SOURCE_MACHINE_ONE = "back.source.machine.one";

	public static final String BACK_SOURCE_MACHINE_FIVE = "back.source.machine.five";

	public static final String BACK_SOURCE_MACHINE_HOUR = "back.source.machine.hour";

	public static final String BACK_PARENT_MISS_MACHINE_ONE = "back.parent.miss.machine.one";

	public static final String BACK_PARENT_MISS_MACHINE_five = "back.parent.miss.machine.five";

	public static final String BACK_PARENT_MISS_MACHINE_HOUR = "back.parent.miss.machine.hour";

	/**
	 * 
	 */
	private String machineId;

	private long timestamp;

	private int type;
	/**
	 * 每页显示的记录数
	 */
	private int size = 10;
	/**
	 * 记录开始的位置，0是从第一条记录开始
	 */
	private int from = 0;
	/**
	 * 9种metric
	 */
	private String metric = BACK_SOURCE_MACHINE_ONE;

	private List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

	@Override
	public String execute() throws Exception {
		try {
			Client client = EsClientFactory.createClient();
			try {
				QueryBuilder machineIdQuery = QueryBuilders.termQuery(
						"machine_id", this.machineId);

				QueryBuilder typeQuery = QueryBuilders.termQuery("type",
						this.type);

				QueryBuilder timestampQuery = QueryBuilders.termQuery(
						"timestamp", this.timestamp);

				QueryBuilder metricQuery = QueryBuilders.termQuery("metric",
						this.metric);

				QueryBuilder allQuery = QueryBuilders.boolQuery().must(
						machineIdQuery).must(typeQuery).must(timestampQuery)
						.must(metricQuery);

				SearchResponse response = client.prepareSearch(INDEX).setQuery(
						allQuery).addSort("topn_num", SortOrder.ASC).setFrom(
						this.from).setSize(this.size).execute().actionGet();

				Iterator<SearchHit> it = response.getHits().iterator();

				while (it.hasNext()) {
					SearchHit sh = it.next();
					result.add(sh.getSource());
				}
			} finally {
				client.close();
			}
		} catch (Exception e) {
		}
		return ActionSupport.SUCCESS;
	}

	@Override
	public void validate() {
		super.validate();
	}

	public List<Map<String, Object>> getResult() {
		return result;
	}

	public void setSize(int size) {
		this.size = size;
	}

	public void setFrom(int from) {
		this.from = from;
	}

	public void setMachineId(String machineId) {
		this.machineId = machineId;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public void setType(int type) {
		this.type = type;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

}
