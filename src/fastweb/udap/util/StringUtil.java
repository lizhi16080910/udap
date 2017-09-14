package fastweb.udap.util;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 名称: StringUtil.java
 * 描述: 字符串读取类
 * 最近修改时间: Oct 10, 201410:36:49 AM
 * @since Oct 10, 2014
 * @author zhangyi
 */
public class StringUtil {

	private static Log log = LogFactory.getLog(StringUtil.class);

	/**
	 * Judge whether a string is null or not
	 * 
	 * @param str
	 * @return
	 */
	public static boolean isEmpty(String str) {
		if (null == str || "".equals(str) || str.length() == 0
				|| str.trim().length() == 0) {
			return true;
		} else {
			return false;
		}
	}

	/**  
	 * @param src  源字符串
	 * @return 字符串，将src的第一个字母转换为大写，src为空时返回null  
	 */
	public static String change(String src) {
		if (src != null) {
			StringBuffer sb = new StringBuffer(src);
			sb.setCharAt(0, Character.toUpperCase(sb.charAt(0)));
			return sb.toString();
		} else {
			return null;
		}
	}

	/**
	 * get the filename of this url
	 * 
	 * @param url
	 * @return the filename of this url
	 */
	public static String getUrlFilename(String url) {
		url = url.replace("http://", "");
		String filename = "";

		int index = url.indexOf('?');
		if (index > -1) {
			url = url.substring(0, index);
		}

		index = url.indexOf(";");
		if (index > -1) {
			url = url.substring(0, index);
		}

		int lastIndex = url.lastIndexOf("/");
		if ((lastIndex + 1) == url.length()) {
			url = url.substring(0, url.length() - 1);
			lastIndex = url.lastIndexOf("/");
		}

		if (lastIndex > -1) {
			filename = url.substring(lastIndex);

		}

		if (filename.contains(".")) {
			filename = filename.substring(0, filename.indexOf("."));
		}

		filename = filename.replaceAll("/", "");

		if ("www".equalsIgnoreCase(filename)) {
			filename = "";
		}

		return filename;
	}

	public static String getLinkTrimDomain(String url) {
		String link = null;
		try {
			String fullDomain = new URL(url).getHost();
			int pos = url.indexOf(fullDomain) + fullDomain.length();
			link = url.substring(pos);
		} catch (MalformedURLException e) {
			log.error("Get link error! url:" + url);
		}
		return link;
	}

	public static String[] getFilenameDirs(String url) {
		String[] filenameDirs = null;
		try {
			String fullDomain = new URL(url).getHost();
			int pos = url.indexOf(fullDomain) + fullDomain.length();
			filenameDirs = url.substring(pos).split("/");
		} catch (Exception e) {
			log.error("Get filenameDirs error! url:" + url);
		}
		return filenameDirs;
	}

	/**
	 * 将中文字符转换为utf-8格式
	 * @param xmlDoc
	 * @return
	 */
	public static String encodeUTF8(String xmlDoc) {
		String str = "";
		try {
			str = URLEncoder.encode(xmlDoc, "utf-8");
			return str;
		} catch (Exception e) {
			str = e.toString();
		}
		return str;
	}

	/**
	 * 将utf-8格式字符转换为中文字符
	 * @param str
	 * @return
	 */
	public static String decodeUTF8(String str) {
		String xmlDoc = "";
		try {
			xmlDoc = URLDecoder.decode(str, "utf-8");
		} catch (Exception e) {
			xmlDoc = e.toString();
		}
		return xmlDoc;
	}
}
