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

/**
 * 违法信息监测/过滤记录管理
 */
@Namespace("/")
@Action(value = "cdnlogismsmonitortest", results = { @Result(name = ActionSupport.SUCCESS, type = "json"), @Result(name = ActionSupport.ERROR, type = "json") })
public class CdnlogISMSMonitorTestQueryAction extends ActionSupport implements ModelDriven<Isms> {

	private static final long serialVersionUID = 1L;
	private final Log log = LogFactory.getLog(getClass());
	private String result;
	private String message;
	private Isms isms = new Isms();
	private static AtomicInteger visitCount = new AtomicInteger(0);
	private static AtomicInteger queryCntThread = new AtomicInteger(3);
	private static Queue<Isms> queue = new ConcurrentLinkedQueue<Isms>();

	@Override
	public String execute() {
		visitCount.incrementAndGet();
		long uuid = getUUID();

		try {
			queue.add(isms);
			log.info("add queue successed" + "------" + uuid);
		} catch (Exception e) {
			log.error("add queue fail-------" + uuid);
			setResult("0");
			return ActionSupport.ERROR;
		}
		if (queryCntThread.get() > 0) {
			// 调用线程启动消费queue队列数据，处理sql，推送文件http
			log.info("-------------------------------提交处理sql子线程-------------------------------");
			CdnlogISMSMonitorAdvanceSqlQuery.getPool().submit(new CdnlogISMSMonitorAdvanceSqlQuery());
			// 递减
			queryCntThread.getAndDecrement();
		}
		log.info("当前http已请求次数: " + visitCount + "------" + queue.size());
		setResult("1");
		return ActionSupport.SUCCESS;
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
		if (StringUtils.isBlank(isms.getDomain())) {
			setMessage("查询domain参数不能为空,多域名,分割");
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

	public long getUUID() {
		Random ran = new Random();
		long time = System.currentTimeMillis() * 10000;
		return (time + ran.nextInt(10000));
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

	public static void setQueue(Queue<Isms> queue) {
		CdnlogISMSMonitorTestQueryAction.queue = queue;
	}

	public static AtomicInteger getQueryCntThread() {
		return queryCntThread;
	}

	public static void setQueryCntThread(AtomicInteger queryCntThread) {
		CdnlogISMSMonitorTestQueryAction.queryCntThread = queryCntThread;
	}

	@Override
	public Isms getModel() {
		return isms;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}
}
