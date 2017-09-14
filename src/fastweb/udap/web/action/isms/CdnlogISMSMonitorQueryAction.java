package fastweb.udap.web.action.isms;

import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;

import com.opensymphony.xwork2.ActionSupport;
import com.opensymphony.xwork2.ModelDriven;

import fastweb.udap.bean.Isms;
import fastweb.udap.util.IpListUtil;
import fastweb.udap.util.StringUtil;

/**
 * 违法信息监测/过滤记录管理
 */
@Namespace("/")
@Action(value = "cdnlogismsmonitor", results = { @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties", "model" }), @Result(name = ActionSupport.ERROR, type = "json", params = { "excludeProperties", "model" }) })
public class CdnlogISMSMonitorQueryAction extends ActionSupport implements ModelDriven<Isms> {
	
	private static final long serialVersionUID = 1L;
	private static Queue<Isms> queue = new ConcurrentLinkedQueue<Isms>();
	private Isms isms = new Isms();
	private final Log log = LogFactory.getLog(getClass());
	private static AtomicInteger visitCount = new AtomicInteger(0);
	//控制可并行运行的线程数
	private static AtomicInteger queryCntThread = new AtomicInteger(2);
	private String message;
	private String result;

	@Override
	public String execute() {

		if (!StringUtil.isEmpty(message)) {
			setResult("0");
			return ActionSupport.ERROR;
		}

		visitCount.incrementAndGet();
		long uuid = getUUID();
		isms.setUuid(uuid);
		
		try {
			if(queue.size() > 5000){
				setResult("0");
				setMessage("当前积压任务大于5000个，请稍后......");
				log.error("add queue fail-------" + uuid + "------当前积压任务大于5000个，请稍后......");
				return ActionSupport.ERROR;
			}
			queue.add(isms);
			log.info("add queue successed" + "------" + uuid);
		} catch (Exception e) {
			log.error("add queue fail-------" + uuid);
			setResult("0");
			return ActionSupport.ERROR;
		}
		if (queryCntThread.get() > 0) {
			//调用线程启动消费queue队列数据，处理sql，推送文件http
			CdnlogISMSMonitorThread.getPool().submit(new CdnlogISMSMonitorThread());
			//递减
			queryCntThread.getAndDecrement();
		}
		log.info("当前http已请求次数: " + visitCount + "------队列积压任务: " + queue.size() + "------当前余下可并发线程数: " + queryCntThread.get());
		setResult("1");
		setMessage("successed");
		return ActionSupport.SUCCESS;
	}

	
	private long getUUID() {
		Random ran = new Random();
		long time = System.currentTimeMillis() * 10000;
		return (time + ran.nextInt(10000));
	}
	
	
	@Override
	public void validate() {
		super.validate();
		if (isms.getBegin() >= isms.getEnd()) {
			log.error("开始时间大于或等于结束时间" + "begin=" + isms.getBegin() + "; end=" + isms.getBegin());
			setMessage("开始时间大于或等于结束时间");
		}
		if (isms.getBegin() == 0 || isms.getEnd() == 0 || (isms.getEnd() - isms.getBegin() < 0) || (isms.getEnd() - isms.getBegin() > 7200)) {
			log.error("begin=" + isms.getBegin() + "; end=" + isms.getEnd() + "查询时间超过2小时");
			setMessage("开始时间结束时间不能为空,结束时间要大于等于开始时间,时间跨度不能超过2小时.");
		}
		if (isms.getId() == 0) {
			setMessage("id不能为空");
		}
		if (StringUtils.isBlank(isms.getType())) {
			setMessage("规则类型type不能为空");
		}
		if ((StringUtils.isBlank(isms.getDomain()) && StringUtils.isBlank(isms.getUrl()) && StringUtils.isBlank(isms.getNodeIpStart()) && StringUtils.isBlank(isms.getNodeIpEnd()) && StringUtils.isBlank(isms.getUserIpStart()) && StringUtils.isBlank(isms.getUserIpEnd()))) {
			setMessage("查询(domain,url,nodeIpStart,nodeIpEnd,userIpStart,userIpEnd)参数不能全为空,用户IP必须成对出现。");
		}
		if (StringUtils.isBlank(isms.getCommandId())) {
			setMessage("commandId不能为空。");
		}
		if (StringUtils.isBlank(isms.getHttpPutUrl())) {
			setMessage("put文件服务器地址httpPutUrl不能为空。");
		}
		if ((StringUtils.isBlank(isms.getUserIpStart()) && StringUtils.isNotBlank(isms.getUserIpEnd())) || (StringUtils.isNotBlank(isms.getUserIpStart()) && StringUtils.isBlank(isms.getUserIpEnd()))) {
			setMessage("用户IP必须成对出现，或者全没有。");
		}
		if (StringUtils.isNotBlank(isms.getUserIpStart()) && StringUtils.isNotBlank(isms.getUserIpEnd()) && (IpListUtil.ip2Long(isms.getUserIpEnd()) - IpListUtil.ip2Long(isms.getUserIpStart()) > 51)) {
			setMessage("用户IP查询位段必须小于50。");
		}
		if ((StringUtils.isBlank(isms.getNodeIpStart()) && StringUtils.isNotBlank(isms.getNodeIpEnd())) || (StringUtils.isNotBlank(isms.getNodeIpStart()) && StringUtils.isBlank(isms.getNodeIpEnd()))) {
			setMessage("节点IP段必须成对出现，或者全没有。");
		}
		if (StringUtils.isNotBlank(isms.getNodeIpStart()) && StringUtils.isNotBlank(isms.getNodeIpEnd()) && (IpListUtil.ip2Long(isms.getNodeIpEnd()) - IpListUtil.ip2Long(isms.getNodeIpStart()) > 20 || (IpListUtil.ip2Long(isms.getNodeIpStart()) - IpListUtil.ip2Long(isms.getNodeIpEnd()) > 0))) {
			setMessage("节点IP段必须小于20,开始段小于结束段。");
		}
	}
	

	public String getMessage() {
		return message;
	}
	public void setMessage(String message) {
		this.message = message;
	}
	public String getResult() {
		return result;
	}
	public void setResult(String result) {
		this.result = result;
	}
	public static Queue<Isms> getQueue() {
		return queue;
	}
	public static AtomicInteger getQueryCntThread() {
		return queryCntThread;
	}
	public static void setQueryCntThread(AtomicInteger queryCntThread) {
		CdnlogISMSMonitorQueryAction.queryCntThread = queryCntThread;
	}
	@Override
	public Isms getModel() {
		return this.isms;
	}
	
	
	
//	老版本方式，没有消息队列处理
//	private static final long serialVersionUID = 1L;
//	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
//	private static AtomicInteger queryCount = new AtomicInteger(5);
//	private static AtomicInteger visitCount = new AtomicInteger(0);
//	public static final String table = "cdnlog_isms_status";
//	private static String IP = "192.168.100.4";
//	private static int PORT = 6999;
//	private final Log log = LogFactory.getLog(getClass());
//	
////	private static Set<String> hostnameSet = new HashSet<String>();
//
//	private String message;
//
//	/* 开始时间 */
//	private long begin = 0;
//	/* 结束时间，时间跨度不超过两小时 */
//	private long end = 0;
//	/* 触发的规则ID，不做查询，只为携带给接口 */
//	private long id = 0;
//	/* 规则类型（监控规则 和 过滤规则） */
//	private String type;
//	/* 节点IP起始段位 */
//	private String nodeIpStart;
//	/* 节点IP结束段位 */
//	private String nodeIpEnd;
//	/* 查询URL */
//	private String url;
//	/* 用户IP开始段位 */
//	private String userIpStart;
//	/* 用户IP结束段位 */
//	private String userIpEnd;
//	/* 查询域名 */
//	private String domain;
//	/* 请求唯一标识 */
//	private String commandId;
//
//	private int limit = 100000;
//	private String httpPutUrl;
//
//	private String result;
//	private String queryInfo;
//
//	@Override
//	public String execute() {
//
//		if (!StringUtil.isEmpty(message)) {
//			setResult("0");
//			return ActionSupport.ERROR;
//		}
//		// log.info(queryInfo);
//
//		// 查询开始时间
//		long beginQuery = System.currentTimeMillis();
//
//		if (queryCount.get() <= 0) {
//			setMessage("one query is running, please wait!!");
//			log.info("one query is running, please wait!!");
//			setResult("0");
//			return ActionSupport.ERROR;
//		}
//
//		queryCount.getAndDecrement();
//		log.info("current queryCount is : " + queryCount.get());
//
//		String sql = queryString();
//		if (sql.equals("error")) {
//			queryCount.incrementAndGet();
//			setResult("0");
//			setMessage("failed");
//			return ActionSupport.ERROR;
//		}
//		log.info(sql);
//		
//		String endTimeStr = DatetimeUtil.timestampToDateStr(this.end, "yyyyMMddHH");
//		String endSystemTimeStr = DatetimeUtil.timestampToDateStr(System.currentTimeMillis()/1000, "yyyyMMddHH");
//		
//		int j = 0;
//		/*随意定一个值,不影响逻辑*/
//		int i = 100;
//		if(!endTimeStr.equals(endSystemTimeStr)){
//			i = sqlExecuteAndJson(sql);
//			j = 1;
//		}
//		log.info("sql返回状态值=" + i + "------是否执行sql状态值(0:没执行;1:执行)=" + j);
//		queryCount.incrementAndGet();
//		// 查询结束时间
//		long endQuery = System.currentTimeMillis();
//		// 打印查询结束时间
//		log.info(("sql query total time(s) " + (endQuery - beginQuery) / (float) 1000));
//
//		Socket client = null;
//		DataOutputStream dos = null;
//		try {
//			// 实例化一个Socket，并指定服务器地址和端口
//			client = new Socket(IP, PORT);
//			dos = new DataOutputStream(client.getOutputStream());
//			log.info("args======" + "cdnlog_isms_monitor" + "#_#_#" + commandId + "#_#_#" + limit + "#_#_#" + httpPutUrl + "#_#_#" + id + "#_#_#" + type + "#_#_#" +  j);
//			dos.writeUTF("cdnlog_isms_monitor" + "#_#_#" + commandId + "#_#_#" + limit + "#_#_#" + httpPutUrl + "#_#_#" + id + "#_#_#" + type + "#_#_#" +  j);
//		} catch (Exception e) {
//			setMessage("发送数据请求失败！");
//			setResult("0");
//			return ActionSupport.ERROR;
//		} finally {
//			if (dos != null) {
//				try {
//					dos.flush(); // 确保所有数据都已经输出
//					dos.close(); // 关闭输出流
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//			if (client != null) {
//				try {
//					client.close(); // 关闭Socket连接
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		setResult("1");
//		setMessage("successed");
//		return ActionSupport.SUCCESS;
//	}
//
//	@Override
//	public void validate() {
//		super.validate();
//
//		log.info("visit number is :" + visitCount.incrementAndGet());
//		if (this.begin >= this.end) {
//			log.error("开始时间大于或等于结束时间" + "begin=" + this.begin + "; end=" + this.end);
//			setMessage("开始时间大于或等于结束时间");
//		}
//		if (begin == 0 || end == 0 || (end - begin < 0) || (end - begin > 7200)) {
//			log.error("begin=" + this.begin + "; end=" + this.end + "查询时间超过2小时");
//			setMessage("开始时间结束时间不能为空,结束时间要大于等于开始时间,时间跨度不能超过2小时.");
//		}
//		if (id == 0) {
//			setMessage("id不能为空");
//		}
//		if (StringUtils.isBlank(type)) {
//			setMessage("规则类型type不能为空");
//		}
//		if ((StringUtils.isBlank(domain) && StringUtils.isBlank(url) && StringUtils.isBlank(nodeIpStart) && StringUtils.isBlank(nodeIpEnd) && StringUtils.isBlank(userIpStart) && StringUtils.isBlank(userIpEnd))) {
//			setMessage("查询(domain,url,nodeIpStart,nodeIpEnd,userIpStart,userIpEnd)参数不能全为空,用户IP必须成对出现。");
//		}
//		if (StringUtils.isBlank(commandId)) {
//			setMessage("commandId不能为空。");
//		}
//		if (StringUtils.isBlank(httpPutUrl)) {
//			setMessage("put文件服务器地址httpPutUrl不能为空。");
//		}
//		if ((StringUtils.isBlank(userIpStart) && StringUtils.isNotBlank(userIpEnd)) || (StringUtils.isNotBlank(userIpStart) && StringUtils.isBlank(userIpEnd))) {
//			setMessage("用户IP必须成对出现，或者全没有。");
//		}
//		if (StringUtils.isNotBlank(userIpStart) && StringUtils.isNotBlank(userIpEnd) && (IpListUtil.ip2Long(userIpEnd) - IpListUtil.ip2Long(userIpStart) > 51)) {
//			setMessage("用户IP查询位段必须小于50。");
//		}
//		// nodeIP
//		if ((StringUtils.isBlank(nodeIpStart) && StringUtils.isNotBlank(nodeIpEnd)) || (StringUtils.isNotBlank(nodeIpStart) && StringUtils.isBlank(nodeIpEnd))) {
//			setMessage("节点IP段必须成对出现，或者全没有。");
//		}
//		if (StringUtils.isNotBlank(nodeIpStart) && StringUtils.isNotBlank(nodeIpEnd) && (IpListUtil.ip2Long(nodeIpEnd) - IpListUtil.ip2Long(nodeIpStart) > 20 || (IpListUtil.ip2Long(nodeIpStart) - IpListUtil.ip2Long(nodeIpEnd) > 0))) {
//			setMessage("节点IP段必须小于20,开始段小于结束段。");
//		}
//		/*
//		if (StringUtils.isNotBlank(nodeIpStart) && StringUtils.isNotBlank(nodeIpEnd)) {
//			initHostname(IpListUtil.ip2Long(nodeIpStart), IpListUtil.ip2Long(nodeIpEnd));
//			if (hostnameSet.size() == 0) {
//				setMessage("节点IP段在BOSS没有对应的主机。");
//			}
//		}
//		*/
//	}
//	
//	
//	private int sqlExecuteAndJson(String sql) {
//		log.info("begin execute insert sql:" + sql);
//		Connection conn = null;
//		Statement stmt = null;
//		int i = 0;
//		try {
//			Class.forName(driverName);
//			conn = ImpalaClientFactory.createClient();
//			stmt = conn.createStatement();
//			i = stmt.executeUpdate(sql);
//		} catch (Exception e) {
//			setMessage(e.getMessage());
//		} finally {
//			try {
//				if (stmt != null) {
//					stmt.close();
//				}
//				if (conn != null) {
//					conn.close();
//				}
//			} catch (SQLException e) {
//				log.error(e.getMessage());
//			}
//		}
//		return i;
//	}
//
//	private String queryString() {
//		try {
//			String time_end = DatetimeUtil.timestampToDateStr(this.end, "yyyyMMddHHmmss");
//			String time_begin = DatetimeUtil.timestampToDateStr(this.begin, "yyyyMMddHHmmss");
//
//			String month_begin = time_begin.substring(0, 6);
//
//			String day_begin = time_begin.toString().substring(6, 8);
//			String hour_begin = time_begin.toString().substring(8, 10);
//
//			String monthCondition_begin = "month_ = '" + month_begin + "'";
//			String dayCondition_begin = "day_ = '" + day_begin + "'";
//			String hourCondition_begin = "hour_ = '" + hour_begin + "'";
//
//			String timeCondition = " timestmp between '" + time_begin + "' and '" + time_end + "'";
//
//			StringBuilder sql = new StringBuilder("insert into table cdnlog_isms_monitor partition(id='" + id + "', commandId='" + commandId + "') select domain,ip,node,count(*) as count, min(timestmp) as timestmp,url from ");
//		
//			sql.append(table + " where 1=1 and " + monthCondition_begin + " and " + dayCondition_begin + " and " + hourCondition_begin);
//
//			if (StringUtils.isNotBlank(domain)) {
//				sql.append(" and domain='" + domain + "'");
//			}
//			if (StringUtils.isNotBlank(url)) {
//				sql.append(" and url='" + url + "'");
//			}
//			if (StringUtils.isNotBlank(nodeIpStart) && StringUtils.isNotBlank(nodeIpEnd)) {
//				sql.append(" and node >= '" + IpListUtil.ip2Long(nodeIpStart) + "' and node <= '" + IpListUtil.ip2Long(nodeIpEnd) + "' ");
//				/*
//				String nodeIPs = "";
//				int size_host = 1;
//				for (String host:hostnameSet) {
//					if (size_host == hostnameSet.size()) {
//						nodeIPs += "'" + host + "'";
//					} else {
//						nodeIPs += "'" + host + "',";
//					}
//					size_host++;
//				}
//				sql.append(" and machine in (" + nodeIPs + ")");
//				*/
//			}
//			if (StringUtils.isNotBlank(userIpStart) && StringUtils.isNotBlank(userIpEnd)) {
//				sql.append(" and ip >=" + IpListUtil.ip2Long(userIpStart) + " and ip <=" + IpListUtil.ip2Long(userIpEnd));
//			}
//			sql.append(" and " + timeCondition);
//
//			sql.append(" group by domain, ip, node, url");
//
//			return sql.toString();
//			/*
//			//union all 相邻的两个小时的数据
//			String time_end = DatetimeUtil.timestampToDateStr(this.end, "yyyyMMddHHmmss");
//			String time_begin = DatetimeUtil.timestampToDateStr(this.begin, "yyyyMMddHHmmss");
//
//			String month_end = time_end.substring(0, 6);
//			String day_end = time_end.toString().substring(6, 8);
//			String hour_end = time_end.toString().substring(8, 10);
//
//			String month_begin = time_begin.substring(0, 6);
//
//			this.month = month_begin;
//
//			String day_begin = time_begin.toString().substring(6, 8);
//			String hour_begin = time_begin.toString().substring(8, 10);
//
//			String monthCondition_end = "month_ = '" + month_end + "'";
//			String dayCondition_end = "day_ = '" + day_end + "'";
//			String hourCondition_end = "hour_ = '" + hour_end + "'";
//
//			String monthCondition_begin = "month_ = '" + month_begin + "'";
//			String dayCondition_begin = "day_ = '" + day_begin + "'";
//			String hourCondition_begin = "hour_ = '" + hour_begin + "'";
//
//			String timeCondition = " t.timestmp between '" + time_begin + "' and '" + time_end + "'";
//
//			StringBuilder sql = new StringBuilder("insert into table cdnlog_isms_monitor partition(month_='" + month_begin + "', time_='" + commandId + "') select '" + id + "','" + type + "',t.domain,t.ip,t.machine,count(*) as count, min(t.timestmp) as timestmp,t.url from (");
//
//			StringBuilder table_temp = new StringBuilder("(select domain, ip, machine, timestmp,url from " + table + " where " + monthCondition_end + " and " + dayCondition_end + " and " + hourCondition_end + ")");
//
//			if (!(month_begin.equals(month_end) && day_begin.equals(day_end) && hour_begin.equals(hour_end))) {
//				table_temp.append(" union all ");
//				table_temp.append("(select domain, ip, machine, timestmp, url from " + table + " where " + monthCondition_begin + " and " + dayCondition_begin + " and " + hourCondition_begin + ")");
//			}
//
//			sql.append(table_temp + ") t where 1=1");
//
//			if (StringUtils.isNotBlank(domain)) {
//				sql.append(" and t.domain='" + domain + "'");
//			}
//			if (StringUtils.isNotBlank(url)) {
//				sql.append(" and t.url='" + url + "'");
//			}
//			if (StringUtils.isNotBlank(nodeIpStart) && StringUtils.isNotBlank(nodeIpEnd)) {
//				String nodeIPs = "";
//				int size_host = 1;
//				for (String host:hostnameSet) {
//					if (size_host == hostnameSet.size()) {
//						nodeIPs += "'" + host + "'";
//					} else {
//						nodeIPs += "'" + host + "',";
//					}
//					size_host++;
//				}
//				sql.append(" and t.machine in (" + nodeIPs + ")");
//			}
//			if (StringUtils.isNotBlank(userIpStart) && StringUtils.isNotBlank(userIpEnd)) {
//				sql.append(" and t.ip >=" + IpListUtil.ip2Long(userIpStart) + " and t.ip <=" + IpListUtil.ip2Long(userIpEnd));
//			}
//			sql.append(" and " + timeCondition);
//
//			sql.append(" group by t.domain, t.ip, t.machine, t.url");
//
//			return sql.toString();
//			*/
//		} catch (Exception e) {
//			log.error("url:" + this.queryInfo);
//			log.error(e);
//			setMessage("error" + e.getMessage());
//			return "error";
//		}
//	}
//
//	public String getMessage() {
//		return message;
//	}
//
//	public void setMessage(String message) {
//		this.message = message;
//	}
//
//	public void setBegin(long begin) {
//		this.begin = begin;
//	}
//
//	public void setEnd(long end) {
//		this.end = end;
//	}
//
//	public void setUrl(String url) {
//		this.url = url;
//	}
//
//	public void setUserIpStart(String userIpStart) {
//		this.userIpStart = userIpStart;
//	}
//
//	public void setUserIpEnd(String userIpEnd) {
//		this.userIpEnd = userIpEnd;
//	}
//
//	public void setNodeIpStart(String nodeIpStart) {
//		this.nodeIpStart = nodeIpStart;
//	}
//
//	public void setNodeIpEnd(String nodeIpEnd) {
//		this.nodeIpEnd = nodeIpEnd;
//	}
//
//	public String getResult() {
//		return result;
//	}
//
//	public void setResult(String result) {
//		this.result = result;
//	}
//
//	public void setId(long id) {
//		this.id = id;
//	}
//
//	public void setType(String type) {
//		this.type = type;
//	}
//
//	public void setCommandId(String commandId) {
//		this.commandId = commandId;
//	}
//
//	public void setDomain(String domain) {
//		this.domain = domain;
//	}
//
//	public void setLimit(int limit) {
//		this.limit = limit;
//	}
//
//	public void setHttpPutUrl(String httpPutUrl) {
//		this.httpPutUrl = httpPutUrl;
//	}
}
