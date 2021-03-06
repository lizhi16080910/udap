package fastweb.udap.web.action.isms;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import fastweb.udap.bean.Isms;
import fastweb.udap.util.DatetimeUtil;
import fastweb.udap.util.IpListUtil;
import fastweb.udap.web.ImpalaClientFactory;

public class CdnlogISMSLogThread implements Runnable {

	private static ExecutorService pool = Executors.newFixedThreadPool(5);
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	private final Log log = LogFactory.getLog(getClass());
	private static final String table = "cdnlog_isms_status";
	private static String IP = "192.168.100.5";
	private static int PORT = 6999;

	public CdnlogISMSLogThread() {
	}

	@Override
	public void run() {
		Queue<Isms> queue = CdnlogISMSLogQueryAction.getQueue();

		log.info("子线程" + Thread.currentThread().getName() + " is beginning!");

		Socket client = null;
		DataOutputStream dos = null;
		while (!queue.isEmpty()) {
			// 一次执行impala JDBC SQL过程 + socket通讯一次读取HDFS对应目录数据
			long uuid = 0L;
			try {
				Isms isms = queue.poll();
				uuid = isms.getUuid();
				// 实例化一个Socket，并指定服务器地址和端口
				client = new Socket(IP, PORT);
				dos = new DataOutputStream(client.getOutputStream());
				String sql = queryString(isms);
				log.info("子线程  " + Thread.currentThread().getName() + " uuid = " + uuid + "---is begin, begin time is:" + System.currentTimeMillis() + "---queue size is:" + queue.size() + "---sql=" + sql);

				String endTimeStr = DatetimeUtil.timestampToDateStr(isms.getEnd(), "yyyyMMddHH");
				long currentT = System.currentTimeMillis() / 1000;
				String endSystemTimeStr = DatetimeUtil.timestampToDateStr(currentT, "yyyyMMddHH");
				/* 重要标识，socket判断是否执行sql，是否读取HDFS OR 直接put空文件 */
				int j = 0;
				/* 随意定一个值,不影响逻辑 */
				int i = 1;
				if (!endTimeStr.equals(endSystemTimeStr)) {
					// 请求URL没有超过一个小时，可以执行SQL
					if (checkExecute(uuid, currentT)) {
						long beginQuery = System.currentTimeMillis();
						log.info("exe sql---" + sql);
						i = sqlExecuteAndJson(sql);
						long endQuery = System.currentTimeMillis();
						log.info("execute impala jdbc sql begin time: " + beginQuery + "---end time: " + endQuery + "sql query total time(s) " + (endQuery - beginQuery) / (float) 1000);
						j = 1;
					}
				}
				log.info("sql返回状态值=" + i + "------是否执行sql状态值(0:没执行;1:执行)=" + j);
				//参数0：为了和违法信息监测/过滤记录管理 查询 socket传递数据格式一致
				log.info("args======" + "cdnlog_isms_log" + "#_#_#" + isms.getCommandId() + "#_#_#" + isms.getLimit() + "#_#_#" + isms.getHttpPutUrl() + "#_#_#" + isms.getId() + "#_#_#" + "0" + "#_#_#" + j);
				dos.writeUTF("cdnlog_isms_log" + "#_#_#" + isms.getCommandId() + "#_#_#" + isms.getLimit() + "#_#_#" + isms.getHttpPutUrl() + "#_#_#" + isms.getId() + "#_#_#" + "0" + "#_#_#" + j);
				log.info("子线程  " + Thread.currentThread().getName() + " uuid = " + uuid + "---is end, end time is:" + System.currentTimeMillis() + "---queue size is:" + queue.size() + "---sql=" + sql);
			} catch (UnknownHostException e) {
				log.error("uuid=" + uuid + "---UnknownHostException---error message:" + e.getMessage());
			} catch (IOException e) {
				log.error("uuid=" + uuid + "---IOException---error message:" + e.getMessage());
			} catch (RuntimeException e) {
				log.error("uuid=" + uuid + "---other Exception---error message:" + e.getMessage());
			} finally {
				if (dos != null) {
					try {
						dos.flush(); // 确保所有数据都已经输出
						dos.close(); // 关闭输出流
					} catch (IOException e) {
						log.error("dos.close() IOException---error message:" + e.getMessage());
					}
				}
				if (client != null) {
					try {
						client.close(); // 关闭Socket连接
					} catch (IOException e) {
						log.error("client.close() IOException---error message:" + e.getMessage());
					}
				}
			}
		}
		/* 恢复静态变量 可并行执行的线程数 */
		CdnlogISMSLogQueryAction.getQueryCntThread().incrementAndGet();
		log.info("子线程" + Thread.currentThread().getName() + "is end!!!!!!!!!!!!!!");
	}

	/* 其它判断条件直接接上 */
	private boolean checkExecute(long uuid, long currentT) {
		if (((currentT - (uuid / 1000 / 10000)) > 3600) || ((currentT - (uuid / 1000 / 10000)) < 0) || (uuid == 0)) {
			// 任务队列积压时间大于1小时，直接返回false，不查询
			return false;
		}
		return true;
	}

	private int sqlExecuteAndJson(String sql) {
		log.info("begin execute insert sql:" + sql);
		Connection conn = null;
		Statement stmt = null;
		int i = 0;
		try {
			Class.forName(driverName);
			conn = ImpalaClientFactory.createClient();
			stmt = conn.createStatement();
			i = stmt.executeUpdate(sql);
		} catch (Exception e) {
		} finally {
			try {
				if (stmt != null) {
					stmt.close();
				}
				if (conn != null) {
					conn.close();
				}
			} catch (SQLException e) {
				log.error(e.getMessage());
			}
		}
		return i;
	}

	private String queryString(Isms isms) {
		String time_end = DatetimeUtil.timestampToDateStr(isms.getEnd(), "yyyyMMddHHmmss");
		String time_begin = DatetimeUtil.timestampToDateStr(isms.getBegin(), "yyyyMMddHHmmss");

		String month_begin = time_begin.substring(0, 6);
		String day_begin = time_begin.toString().substring(6, 8);
		String hour_begin = time_begin.toString().substring(8, 10);

		String monthCondition_begin = "month_ = '" + month_begin + "'";
		String dayCondition_begin = "day_ = '" + day_begin + "'";
		String hourCondition_begin = "hour_ = '" + hour_begin + "'";

		String timeCondition = " timestmp between '" + time_begin + "' and '" + time_end + "'";

		StringBuilder sql = new StringBuilder("insert into table cdnlog_isms_log partition(id='" + isms.getId() + "', commandId='" + isms.getCommandId() + "') select node,ip,timestmp,url from ");

		sql.append(table + " where " + monthCondition_begin + " and " + dayCondition_begin + " and " + hourCondition_begin);

		if (StringUtils.isNotBlank(isms.getUrl())) {
			sql.append(" and url='" + isms.getUrl() + "'");
		}
		if (StringUtils.isNotBlank(isms.getNodeIpStart()) && StringUtils.isNotBlank(isms.getNodeIpEnd())) {
			sql.append(" and node >= '" + IpListUtil.ip2Long(isms.getNodeIpStart()) + "' and node <= '" + IpListUtil.ip2Long(isms.getNodeIpEnd()) + "' ");
		}
		if (StringUtils.isNotBlank(isms.getUserIpStart()) && StringUtils.isNotBlank(isms.getUserIpEnd())) {
			sql.append(" and ip >=" + IpListUtil.ip2Long(isms.getUserIpStart()) + " and ip <=" + IpListUtil.ip2Long(isms.getUserIpEnd()));
		}
		sql.append(" and " + timeCondition);
		return sql.toString();
	}

	public static ExecutorService getPool() {
		return pool;
	}

	public static void setPool(ExecutorService pool) {
		CdnlogISMSLogThread.pool = pool;
	}
}
