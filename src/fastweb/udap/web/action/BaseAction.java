package fastweb.udap.web.action;

import java.util.HashMap;
import java.util.Map;

import com.opensymphony.xwork2.ActionSupport;

public class BaseAction extends ActionSupport{

	private static final long serialVersionUID = 1L;
	
	protected static String CLUSTER_NAME = "cluster.name";
	protected static String ELASTICSEARCH_CDNLOG = "elasticsearch_cdnlog";
	protected static String HOST_NAME = "192.168.100.205";
	protected static int PORT = 9300;
	
	protected static Map<String,String> metrice = new HashMap<String, String>();
	
	static{
		metrice.put("CDNLOG_BADREQ_COUNT_DAY", "cdnlog.badreq.count.day");
		metrice.put("CDNLOG_BADREQ_COUNT_HOUR", "cdnlog.badreq.count.hour");
		metrice.put("NOHIT_MACHINE_COUNT", "nohit.machine.count");
		metrice.put("NOHIT_DOMAIN_COUNT", "nohit.domain.count");
		metrice.put("UA_COUNT", "ua.count");
	}
}
