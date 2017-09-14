/**
 * 
 */
package fastweb.udap.util;

import java.util.regex.Pattern;

/**
 * @author lfq
 *
 */
public class IPUtil {

	/**
	 * 
	 * @param ipInt
	 *            long
	 * @return String
	 * @description 将long型IP地址转换为点分制字符串
	 */
	public static String int2ip(long ipInt) {
		StringBuilder sb = new StringBuilder();
		sb.append((ipInt >> 24) & 0xFF).append(".");
		sb.append((ipInt >> 16) & 0xFF).append(".");
		sb.append((ipInt >> 8) & 0xFF).append(".");
		sb.append(ipInt & 0xFF);
		return sb.toString();
	}

	/**
	 * 
	 * @param strIp
	 *            String
	 * @return long
	 * @description 将点分制字符串IP转换为long
	 */
	public static long string2ip(String strIp) {
		Pattern pattern = Pattern
				.compile("^((\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]"
						+ "|[*])\\.){3}(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]|[*])$");
		if (!checkIP(strIp)) {
			return -1;
		}

		long[] ip = new long[4];
		// 先找到IP地址字符串中.的位置
		int position1 = strIp.indexOf(".");
		int position2 = strIp.indexOf(".", position1 + 1);
		int position3 = strIp.indexOf(".", position2 + 1);
		// 将每个.之间的字符串转换成整型
		ip[0] = Long.parseLong(strIp.substring(0, position1));
		ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
		ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
		ip[3] = Long.parseLong(strIp.substring(position3 + 1));
		return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
	}

	/**
	 * 
	 * @param strIp
	 *            String
	 * @return boolean
	 * @description 检测点分制IP是否合法
	 */
	public static boolean checkIP(String strIp) {
		Pattern pattern = Pattern
				.compile("^((\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]"
						+ "|[*])\\.){3}(\\d|[1-9]\\d|1\\d\\d|2[0-4]\\d|25[0-5]|[*])$");
		return pattern.matcher(strIp).matches();
	}
}
