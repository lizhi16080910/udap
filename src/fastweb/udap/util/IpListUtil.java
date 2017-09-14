package fastweb.udap.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IpListUtil {
	
	private final static Log log = LogFactory.getLog(IpListUtil.class);
	
	private static long timeinterval = 1 * 60 * 60 * 1000L;
	
	private static long lastUpdate = System.currentTimeMillis();
	
	private static String CDN_IP_LIST_URL = "http://192.168.100.204:8088/ip_list.gz";
	
	private static Map<String, String> IP_HOST = new HashMap<String, String>();
	private static Map<String, String> Host_IP = new HashMap<String, String>();

	static {
		update();
	}

	public static void update() {
		URL url = null;
		GZIPInputStream gzs = null;
		BufferedReader reader = null;
		try {
			url = new URL(CDN_IP_LIST_URL);
			gzs = new GZIPInputStream(url.openStream());
			reader = new BufferedReader(new InputStreamReader(gzs));
			String line = "";
			while ((line = reader.readLine()) != null) {
				String[] ops = line.split(",");
				if (ops.length == 9) {
					IP_HOST.put(ops[6], ops[0]);
					Host_IP.put(ops[0], ops[6]);
				}
			}
		} catch (MalformedURLException e) {
			log.error("MalformedURLException---url parse error!!!!!!");
		} catch (IOException e) {
			log.error("IOException GZIPInputStream is failed!!!!!!");
		}
	}

	public static String getHostname(String ip) {
		long currentTime = System.currentTimeMillis();
		if ((currentTime - lastUpdate) > timeinterval) {
			lastUpdate = currentTime;
			update();
		}
		return IP_HOST.get(ip);
	}
	
	public static String getIp(String hostname) {
		long currentTime = System.currentTimeMillis();
		if ((currentTime - lastUpdate) > timeinterval) {
			lastUpdate = currentTime;
			update();
		}
		return Host_IP.get(hostname);
	}
	
	public static Long ip2Long(String ip){
		String[] ipNums = ip.split("\\.");
		return (Long.parseLong(ipNums[0]) << 24) + (Long.parseLong(ipNums[1]) << 16) + (Long.parseLong(ipNums[2]) << 8) + (Long.parseLong(ipNums[3]));
	}

	
	public static String ipToString(long ipaddress) {
		StringBuffer sb = new StringBuffer("");
		sb.append(String.valueOf((ipaddress >>> 24)));
		sb.append(".");
		sb.append(String.valueOf((ipaddress & 0x00FFFFFF) >>> 16));
		sb.append(".");
		sb.append(String.valueOf((ipaddress & 0x0000FFFF) >>> 8));
		sb.append(".");
		sb.append(String.valueOf((ipaddress & 0x000000FF)));
		return sb.toString();
	}
	
	public static void main(String[] args) {
		System.out.println(getHostname("122.228.199.122"));
	}
}
