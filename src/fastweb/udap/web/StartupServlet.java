package fastweb.udap.web;

import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import fastweb.udap.cache.CacheLoader;


/**
 * 系统初始化加载对象
 */
public class StartupServlet extends HttpServlet {
	
	private static final long serialVersionUID = 9081356816193327531L;
	
	@SuppressWarnings("unused")
	private final Log log = LogFactory.getLog(getClass());
	public static Map<String,String> userMap = new HashMap<String,String>();
	
	@Override
	public void init() throws ServletException {
		System.out.println("-----------------StartupServlet------------------");
		CacheLoader.initFileCache();
	}
	
	
	public static void main(String[] args) {
//		readUserXml();
	}
	
}
