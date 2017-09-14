package fastweb.udap.web.action;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
@Action(value = "cdnlogmediaplayaction", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {}) })
public class CdnlogMediaPlayAction extends ActionSupport {
	private static final long serialVersionUID = 1L;
	private final Log log = LogFactory.getLog(getClass());
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	//query param
	public static final String table = "medialog";
	public static final String table_m = "medialog_merge";
	public static final String coloumn="bitrate,minutecs,localtime as time";
	
	/* 时间戳 */
	private String time;
	/* 域名 */
	private String url;
	private String status;
	private Long start;
	private Long end;
	private String query_table;
	
	private String ip="";
	

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
		StringBuffer listDay = null; 
		Class.forName(driverName);

		//将unix时间转换为date
		String startTime = DatetimeUtil.timestampToDateStr(start, "yyyyMMddHHmmss");
		String endTime = DatetimeUtil.timestampToDateStr(end, "yyyyMMddHHmmss");
		
		//取得查询年月
		String queryStartTime = startTime.substring(0,8);
		String queryEndtTime = endTime.toString().substring(0,8);
		
		//取得查询月份
		String startMonth = startTime.toString().substring(0,6);
		String endMonth = endTime.toString().substring(0,6);
		
		//取得查询天
		String startDay = startTime.toString().substring(6,8);
		String endDay = endTime.toString().substring(6,8);
		
		//根据查询日期，判断需查询合并后的表，还是当前表
		String Today =  DatetimeUtil.getTodayDate();
		if(Today.equals(startMonth+startDay)){
			query_table = table;
		}else{
			query_table = table_m;
		}
		
		
		//取得查询小时
		String startDayHour = startTime.toString().substring(8,10);
		String endDayHour = endTime.toString().substring(8,10);
		
		StringBuilder sql = new StringBuilder("");
		
		//select bitrate,minutecs,minute_  from medialog where  month_='201506' and day_='09' and url='s.vdo17v.com/agin/4' and ip=1857654021  and concat(month_,day_,hour_,minute_) between '201506091201' and '201506090600'
		sql.append( "select ").append(coloumn).append(" from " ).append(query_table).append( " where  1=1 " );
		//如果查询数据区间为同一个年月，则进行分区查询优化
		if(startMonth.equals(endMonth)){
			sql.append(" and month_ = '").append(startMonth).append("'");
			//如果查询数据区间为同一天，则进行分区查询优化
			if(queryStartTime.equals(queryEndtTime)){
				sql.append(" and day_ = '").append(startDay).append("'");
				//如果查询数据区间为同一天中同一小时，则进行分区查询优化
				if(startDayHour.equals(endDayHour)){
					sql.append(" and hour_ = '").append(startDayHour).append("'");
				}
			}else{
				//如果不为同一天，但查询月份相同的话，取中间的每一天
				if(startMonth.equals(endMonth)){
					listDay = DatetimeUtil.getListDay(startTime, endTime);
					sql.append(" and day_ in ").append(listDay);
				}
			}
		}else{
			//考虑跨月查询的情况
			//201505 201506
			sql.append(" and month_ in ('").append(startMonth).append("',");
			sql.append("  '").append(endMonth).append("')");
			
		} 
		if(ip.length()>1) sql.append(" and ip = ").append(ip);
		sql.append(" and url = '").append(url).append("'");
		sql.append(" and command = '").append((status).toUpperCase()).append("'");
		sql.append(" and localtime between '" ).append( startTime ).append( "' and '" ).append( endTime );
		sql.append( "'");
		
		log.info(sql);
		//查询开始时间
		long startQuery = System.currentTimeMillis();
			
		try {
			conn = ImpalaClientFactory.createClient();
			Statement stmt = conn.createStatement();
		
			
			ResultSet rs = stmt.executeQuery(sql.toString());
			//查询结束时间
			long endQuery = System.currentTimeMillis();
			
			log.info(("sql query total time(s) " + (endQuery - startQuery) / 1000));
			
			ResultSetMetaData md = rs.getMetaData();
			
			int num = md.getColumnCount();
			
			while (rs.next()) {
				total++;
				JSONObject mapOfColValues = new JSONObject();
				for ( int i = 1; i <= num; i++) {
					if (md.getColumnName(i).toString().equals("time")) {
						mapOfColValues.put(md.getColumnName(i),DatetimeUtil.impalaStrToDateLong(rs.getString(i)));
					} else {
						mapOfColValues.put(md.getColumnName(i), rs.getObject(i));
					}
				}
				
				result.add(mapOfColValues);
			}
			
			result.remove(ip);
		
			rs.close();
			stmt.close();
			//查询结束时间
			long endQuery2 = System.currentTimeMillis();
			
			log.info(("java generate object time(s) " + (endQuery2 - endQuery) / 1000));
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			conn.close();
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

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

}
