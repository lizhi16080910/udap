package fastweb.udap.web.action;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.ServletActionContext;
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
@Action(value = "cdnlogqueryaction", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = {}) })
public class CdnlogQueryAction extends ActionSupport {
	private static final long serialVersionUID = 1L;
	private final Log log = LogFactory.getLog(getClass());
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	
	//query param
	public static final String table = "cdnlog";
	public static final String table_m = "cdnlog_merge";
	private StringBuffer coloumn= new StringBuffer();
	
	private String query_table;
	
	/* 时间戳 */
	private String time;
	private int limit;
	private List cList = new  ArrayList(); ;
	private List countList = new  ArrayList();
	private List sumList = new  ArrayList(); ;
	private Long start;
	private Long end;
	private StringBuffer listDay = null; 
	private StringBuffer orderGroupSql =  new StringBuffer();
	
	
	

	/* 每页显示条数 */
	// private int size = 10;
	/* 开始的记录数 */
	private int from = 0;
	/* 条件下总条数 */
	private long total;


	JSONArray result = new JSONArray();

	@Override
	public String execute() throws Exception {
		String query = queryString();
		
		//校验时间,查询sql
		if(query.equals("error") || cList.isEmpty() || (end-start)/60/60/24>7){
			return  ActionSupport.SUCCESS;
		}
		 
		
		
		for(int i = 0; i < cList.size(); i++){
			if(cList.get(i).equals("timestamp")){
				coloumn.append(" as time ");
			}else if(countList.contains(cList.get(i))){
				coloumn.append("count(").append(cList.get(i));
			}else if(sumList.contains(cList.get(i))){
				coloumn.append("sum(").append(cList.get(i));
			}else{
				coloumn.append(cList.get(i));
			}
			
			if(countList.contains(cList.get(i)) || sumList.contains(cList.get(i))){
				coloumn.append(") as ");
				coloumn.append(cList.get(i));
			}
			
			coloumn.append(","); 
		}
		
		
		Connection conn = null;
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
		
		//取得查询小时
		String startDayHour = startTime.toString().substring(8,10);
		String endDayHour = endTime.toString().substring(8,10);
		
		//根据查询日期，判断需查询合并后的表，还是当前表
		String Today =  DatetimeUtil.getTodayDate();
		if(Today.equals(startMonth+startDay)){
			query_table = table;
		}else{
			query_table = table_m;
		}
		
		StringBuilder sql = new StringBuilder("");

		sql.append( "select ").append(coloumn.deleteCharAt(coloumn.length()-1)).append(" from " ).append(query_table).append( " where  1=1 " );
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
		sql.append(query);
		sql.append(" and timestmp between '" ).append( startTime ).append( "' and '" ).append( endTime ).append( "' ");
		sql.append(orderGroupSql);
		sql.append(" limit  ").append(limit);
		
		
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
			
		
			rs.close();
			stmt.close();
			//查询结束时间
			long endQuery2 = System.currentTimeMillis();
			
			log.info(("java generate object time(s) " + (endQuery2 - endQuery) / 1000));

		} catch (Exception e) {
			 log.info(e.toString());
			 e.printStackTrace();
			 
		} finally {
			
			if(conn != null){
				conn.close();
			}
		}
		return ActionSupport.SUCCESS;
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

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	public List getcList() {
		return cList;
	}

	public void setcList(List cList) {
		this.cList = cList;
	}
	

	@SuppressWarnings({ "null", "unchecked" })
	public String queryString() throws Exception{
		try{
			HashMap<String, String> termMap = new HashMap<String, String>();
			HashMap<String, String> queryMap = new HashMap<String, String>();
			
			StringBuffer query = new StringBuffer();
			StringBuffer orderSql = new StringBuffer();
			StringBuffer groupSql = new StringBuffer();
			orderGroupSql.append(" ");
			
			List orderList= new ArrayList(); 
			List groupList = new  ArrayList();
			List countList_T = new  ArrayList();
			List sumList_T = new  ArrayList();
			
			termMap.put("1", "=");
			termMap.put("2", ">");
			termMap.put("3", "<");
			termMap.put("4", "like");
			termMap.put("5", "group by");
			termMap.put("6", "order by");
			termMap.put("7", "order by desc");
			termMap.put("8", "count");
			termMap.put("9", "sum");
			
			//获取查询URL
			HttpServletRequest request=ServletActionContext.getRequest(); 
			String path=request.getRequestURI(); 
			String actionPath=".."+path.substring(9);  
			String queryInfo=request.getQueryString();
			if(queryInfo!=null&&(!queryInfo.equals(""))){  
				  actionPath=actionPath+"?"+queryInfo;  
			} 
			String[] queryList = queryInfo.split("&");
			
			
			
		   for(int i=0;i<queryList.length;i++){ 
			   String innleQueryList[] = queryList[i].split("=");
			   if(innleQueryList[0].startsWith("term") && innleQueryList[1].equals("5")){
				   groupList.add(innleQueryList[0].toString());
			   }else if(innleQueryList[0].startsWith("term") && innleQueryList[1].equals("6")){
				   orderList.add(innleQueryList[0].toString());
				   queryMap.put(innleQueryList[0], innleQueryList.length<2?"":innleQueryList[1]);
			   }else if(innleQueryList[0].startsWith("term") && innleQueryList[1].equals("7")){
				   orderList.add(innleQueryList[0].toString());
				   queryMap.put(innleQueryList[0], innleQueryList.length<2?"":innleQueryList[1]);
			   }else if(innleQueryList[0].startsWith("term") && innleQueryList[1].equals("8")){
				   countList_T.add(innleQueryList[0].toString());
				   queryMap.put(innleQueryList[0], innleQueryList.length<2?"":innleQueryList[1]);
			   }else if(innleQueryList[0].startsWith("term") && innleQueryList[1].equals("9")){
				   sumList_T.add(innleQueryList[0].toString());
				   queryMap.put(innleQueryList[0], innleQueryList.length<2?"":innleQueryList[1]);
			   }else if(innleQueryList[0].equals("cList")){
				   cList.add(innleQueryList[1]);
				   queryMap.put(innleQueryList[0], innleQueryList.length<2?"":innleQueryList[1]);
			   }else{
				   queryMap.put(innleQueryList[0], innleQueryList.length<2?"":innleQueryList[1]);
			   }
		       
	        }
		   
		   //拼凑，order by group by 语句
		   for(int i = 0; i < orderList.size(); i++){  
			   String idx = orderList.get(i).toString().substring(4, 5);
			   //移除order group 查询字段
			   
			   queryMap.remove("text"+idx);
			   
			   orderSql.append(queryMap.get("col"+idx));
			   
			   if(queryMap.get("term"+idx).equals("7")){
				   orderSql.append(" desc ");
			   }
			   
			   orderSql.append(" ,");
			   
			   queryMap.remove("term"+idx);
	        }
		   
		   for(int i = 0; i < groupList.size(); i++){  
			   String idx = groupList.get(i).toString().substring(4, 5);
			   //移除order group 查询字段
			   queryMap.remove("term"+idx);
			   queryMap.remove("text"+idx);
			   groupSql.append(queryMap.get("col"+idx)+" ,");
			   
	        }
		   
		   
		   
		   for(int i = 0; i < orderList.size(); i++){  
			   String idx = orderList.get(i).toString().substring(4, 5);
			   //移除order group 查询字段
			   queryMap.remove("col"+idx);
	        }
		   
		   for(int i = 0; i < groupList.size(); i++){  
			   String idx = groupList.get(i).toString().substring(4, 5);
			   //移除order group 查询字段
			   queryMap.remove("col"+idx);
			   
	        }
		   
		   //移除count字段
		   for(int i = 0; i < countList_T.size(); i++){  
			   String idx = countList_T.get(i).toString().substring(4, 5);
			   countList.add(queryMap.get("col"+idx));
			   queryMap.remove("col"+idx);
			   queryMap.remove("term"+idx);
			   queryMap.remove("text"+idx);
	        }
		   
		 //移除sum字段
		   for(int i = 0; i < sumList_T.size(); i++){  
			   String idx = sumList_T.get(i).toString().substring(4, 5);
			   sumList.add(queryMap.get("col"+idx));
			   queryMap.remove("col"+idx);
			   queryMap.remove("term"+idx);
			   queryMap.remove("text"+idx);
	        }
		   
		   
		   //查询条件拼凑
		   for(int i=0;i<queryMap.size()/2;i++){ 
			   if(queryMap.containsKey("col"+i)){
				   query.append(" and ");
				   
				   //取查询字段
				   query.append(queryMap.get("col"+i)+" ");
				   
				   //取查询判断条件，like,=,>...
				   query.append(termMap.get(queryMap.get("term"+i))+" ");
				   
				   
				   
				   //判断数字类型的查询字段，去掉'
				   if(queryMap.get("col"+i).equals("ip") || queryMap.get("col"+i).equals("cs") || queryMap.get("col"+i).equals("es")
						   || queryMap.get("col"+i).equals("dd")){
					   
				   }else{
					   query.append("'");
				   }
				   
				   //假如like ,加上%
				   if(termMap.get(queryMap.get("term"+i)).equals("like")){
					   query.append("%");
				   }
				   
				   query.append(queryMap.get("text"+i));
				   
				   //假如like ,加上%
				   if(termMap.get(queryMap.get("term"+i)).equals("like")){
					   query.append("%");
				   }
				   
				 //判断数字类型的查询字段，去掉后'
				   if(queryMap.get("col"+i).equals("ip") || queryMap.get("col"+i).equals("cs") || queryMap.get("col"+i).equals("es")
						   || queryMap.get("col"+i).equals("dd")){
				   }else{
					   query.append("'");
				   }
				  
				   
			   }
		   }
		   
		   //加上order by gourp by
		   if(groupSql.length()>0){
			   orderGroupSql.append(" group by " +groupSql.deleteCharAt(groupSql.length()-1));
		   }
		   if(orderSql.length()>0){
			   orderGroupSql.append(" order by " +orderSql.deleteCharAt(orderSql.length()-1));
		   }
		   
			return query.toString();
			
		}catch(Exception e){
			return "error";
		}
	}
}
