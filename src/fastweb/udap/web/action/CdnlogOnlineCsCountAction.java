package fastweb.udap.web.action;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.DatetimeUtil;
import fastweb.udap.web.ImpalaClientFactory;

/**
 * @author impala jdbc 查询
 */
@Namespace("/")
@Action(value = "cdnlogonlinecscountaction", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {}) })
public class CdnlogOnlineCsCountAction extends ActionSupport {
	private static final long serialVersionUID = 1L;
	private final Log log = LogFactory.getLog(getClass());
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	// query param
	public static final String table = "cdnlog_media_cscount";
	public static final String coloumn = "result as count,time";

	/* 时间戳 */
	private String time;
	/* 域名 */
	private String url;
	private String status;
	private Long start;
	private Long end;
	StringBuffer listDay = null;

	/* 每页显示条数 */
	// private int size = 10;
	/* 开始的记录数 */
	private int from = 0;
	/* 条件下总条数 */
	private long total;

	JSONArray result = new JSONArray();

	@Override
	public String execute() throws Exception {

		Connection conn = null;
		Class.forName(driverName);

		// 将unix时间转换为date
		String startTime = DatetimeUtil.timestampToDateStr(start,
				"yyyyMMddHHmm");
		String endTime = DatetimeUtil.timestampToDateStr(end, "yyyyMMddHHmm");

		// 取得查询年月
		String queryStartTime = startTime.substring(0, 8);
		String queryEndtTime = endTime.toString().substring(0, 8);

		// 取得查询月份
		String startMonth = startTime.toString().substring(0, 6);
		String endMonth = endTime.toString().substring(0, 6);

		// 取得查询天
		String startDay = startTime.toString().substring(6, 8);
		String endDay = endTime.toString().substring(6, 8);

		StringBuilder sql = new StringBuilder("");

		// select address,type,result,time from cdnlog_media_cscount where 1=1
		// and month_ = '201506' and day_ = '08' and address = 's.vdo17v.com'
		// and time between '201506080455' and '201506080500'
		sql.append("select ").append(coloumn).append(" from ").append(table)
				.append(" where 1=1 ");
		// 如果查询数据区间为同一个年月，则进行分区查询优化
		if (startMonth.equals(endMonth)) {
			sql.append(" and month_ = '").append(startMonth).append("'");
			// 如果查询数据区间为同一天，则进行分区查询优化
			if (queryStartTime.equals(queryEndtTime)) {
				sql.append(" and day_ = '").append(startDay).append("'");
			} else {
				// 如果不为同一天，但查询月份相同的话，取中间的每一天
				if (startMonth.equals(endMonth)) {
					listDay = DatetimeUtil.getListDay(startTime, endTime);
					sql.append(" and day_ in ").append(listDay);
				}

			}
		} else {
			// 考虑跨月查询的情况
			// 201505 201506
			sql.append(" and  month_ in ('").append(startMonth).append("',");
			sql.append("  '").append(endMonth).append("')");

		}
		sql.append(" and type = ").append(status);
		sql.append(" and address = '").append(url).append("'");
		sql.append(" and time between '").append(startTime).append("' and '")
				.append(endTime);
		sql.append("'");

		log.info(sql);

		try {
			conn = ImpalaClientFactory.createClient();
			try {
				Statement stmt = conn.createStatement();
				try {
					ResultSet rs = stmt.executeQuery(sql.toString());
					try {

						ResultSetMetaData md = rs.getMetaData();
						int num = md.getColumnCount();

						while (rs.next()) {
							total++;
							JSONObject mapOfColValues = new JSONObject();
							for (int i = 1; i <= num; i++) {
								if (md.getColumnName(i).toString().equals(
										"time")) {
									mapOfColValues
											.put(
													md.getColumnName(i),
													DatetimeUtil
															.impalaStrToDateLongMinute(rs
																	.getString(i)));
								} else {
									mapOfColValues.put(md.getColumnName(i), rs
											.getObject(i));
								}
							}
							result.add(mapOfColValues);
						}
					} finally {
						rs.close();
					}
				} finally {
					stmt.close();
				}
			} finally {
				conn.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ActionSupport.SUCCESS;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public Long getStart() {
		return start;
	}

	public void setStart(Long start) {
		this.start = start;
	}

	public Long getEnd() {
		return end;
	}

	public void setEnd(Long end) {
		this.end = end;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public void validate() {
		super.validate();
	}

	public JSONArray getResult() {
		return result;
	}

	public void setResult(JSONArray result) {
		this.result = result;
	}

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

}
