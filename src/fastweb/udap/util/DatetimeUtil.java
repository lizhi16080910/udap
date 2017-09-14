package fastweb.udap.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;

/**
 * 名称: DatetimeUtil.java 描述: 日期时间转换工具类 最近修改时间: Oct 10, 201410:36:04 AM
 * 
 * @since Oct 10, 2014
 * @author zhangyi
 */
public class DatetimeUtil {

	// 格式化类型：年月日（yyyyMMdd）
	public static final String yyyyMMdd = "yyyyMMdd";

	// 格式化类型：年月日时分秒（yyyy-MM-dd HH:mm:ss）
	public static final String yyyy_MM_dd_HH_mm_ss = "yyyy-MM-dd HH:mm:ss";

	// 格式化类型：年月日时分秒（yyyyMMddHHmmss）
	public static final String yyyyMMddHHmmss = "yyyyMMddHHmmss";

	/**
	 * Compute specified {@link Date} using {@link Calendar} class. For example:<br>
	 * if howManyDays=-2, date="2009-06-04 18:21:46", then return value is
	 * "2009-06-02 18:21:46".
	 * 
	 * @param howManyDays
	 *            if howManyDays<0 then Math.abs(howManyDays) days ago.
	 * @param date
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static Date getDatetime(int howManyDays, Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, howManyDays);
		Date beforeDate = calendar.getTime();
		beforeDate.setHours(date.getHours());
		beforeDate.setMinutes(date.getMinutes());
		beforeDate.setSeconds(date.getSeconds());
		return beforeDate;
	}

	/**
	 * Compute specified {@link Date} using {@link Calendar} class. For example:<br>
	 * if howManyDays=-2, current date time is "2012-06-04 18:21:46", then
	 * return value is "2012-06-02 18:21:46".
	 * 
	 * @param howManyDays
	 *            if howManyDays<0 then Math.abs(howManyDays) days ago.
	 * @return
	 */
	public static Date getDatetime(int howManyDays) {
		return getDatetime(howManyDays, new Date());
	}

	/**
	 * Compute specified {@link Date} according to date string and date format.<br>
	 * For example:<br>
	 * if dateString="2012/06/04 12:54:31", format='yyyy/MM/dd HH:mm:ss', then
	 * return the format of {@link Date} representation.
	 * 
	 * @param dateString
	 * @param format
	 * @return
	 */
	public static Date getDatetime(String dateString, String format) {
		DateFormat df = new SimpleDateFormat(format);
		try {
			return df.parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Compute String format of {@link Date} according to using {@link Calendar}
	 * class and a given date time format.
	 * 
	 * @param howManyDays
	 * @param date
	 * @param format
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static String formatDatetime(int howManyDays, Date date, String format) {
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_MONTH, howManyDays);
		Date beforeDate = calendar.getTime();
		beforeDate.setHours(date.getHours());
		beforeDate.setMinutes(date.getMinutes());
		beforeDate.setSeconds(date.getSeconds());
		return formatDateTime(beforeDate, format);
	}

	/**
	 * Compute String format of {@link Date}.
	 * 
	 * @param date
	 * @param format
	 * @return
	 */
	public static String formatDateTime(Date date, String format) {
		DateFormat df = new SimpleDateFormat(format);
		String dateString = df.format(date);
		return dateString;
	}

	/**
	 * 获取当前日期yyyymmdd
	 * */
	public static String getTodayDate() {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");// 设置日期格式
		return df.format(new Date());
	}

	/**
	 * 获取格式化日期字符串
	 * */
	public static String getFormatDate(Date date, String type) {
		String result = null;
		if (null == date) {
			return null;
		} else {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(type);
			result = simpleDateFormat.format(date);
		}
		return result;
	}

	/**
	 * 字符串转换日期
	 * */
	public static Date toDate(String dateStr, String type) {
		SimpleDateFormat format = new SimpleDateFormat(type);
		try {
			return format.parse(dateStr);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * 将String格式日期转为Date
	 * 
	 * @param strDate
	 * @return Date
	 */
	public static Date strToDateLong(String strDate) {
		Date date = null;
		if (strDate == null || strDate.length() == 0) {
			return date;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			return sdf.parse(strDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

	/**
	 * 将String格式日期转为Date
	 * 
	 * @param strDate
	 * @return Date
	 */
	public static Date strToDate(String strDate) {
		Date date = null;
		if (strDate == null || strDate.length() == 0) {
			return date;
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			return sdf.parse(strDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

	/**
	 * 将String格式日期转为Date
	 * 
	 * @param strDate
	 * @return Date
	 */
	public static String impalaStrToDateLong(String strDate) throws Exception {
		long epoch = 0;
		// 2015 07 01 20 15 12
		int year = Integer.valueOf(strDate.substring(0, 4));
		int month = Integer.valueOf(strDate.substring(4, 6)) - 1;
		int day = Integer.valueOf(strDate.substring(6, 8));
		int hour = Integer.valueOf(strDate.substring(8, 10));
		int minute = Integer.valueOf(strDate.substring(10, 12));
		int second = Integer.valueOf(strDate.substring(12, 14));
		Calendar calendar = Calendar.getInstance();
		calendar.set(year, month, day, hour, minute);
		calendar.set(Calendar.SECOND, second);
		epoch = calendar.getTimeInMillis() / 1000;
		return String.valueOf(epoch);
	}

	/**
	 * 将String格式日期转为Date
	 * 
	 * @param strDate
	 * @return Date
	 */
	public static String impalaStrToDateLongMinute(String strDate) throws Exception {
		long epoch = 0;
		// 2015 07 01 20 15
		int year = Integer.valueOf(strDate.substring(0, 4));
		int month = Integer.valueOf(strDate.substring(4, 6)) - 1;
		int day = Integer.valueOf(strDate.substring(6, 8));
		int hour = Integer.valueOf(strDate.substring(8, 10));
		int minute = Integer.valueOf(strDate.substring(10, 12));

		Calendar calendar = Calendar.getInstance();
		// calendar.setTime(sdf.parse(strDate));
		// calendar.add(Calendar.MONTH, 6);
		calendar.set(year, month, day, hour, minute);
		calendar.set(Calendar.MILLISECOND, 0);

		epoch = calendar.getTimeInMillis() / 1000;

		return String.valueOf(epoch);
	}

	/**
	 * 将String格式日期转为Date
	 * 
	 * @param timeFormat
	 * @return Date
	 */
	public static Date strToDate(String strDate, String timeFormat) {
		Date date = null;
		if (strDate == null || strDate.length() == 0) {
			return date;
		}
		SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
		try {
			return sdf.parse(strDate);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
	}

	/**
	 * 将String格式日期转为Date, 验证是否合法的字符串
	 * 
	 * @param timeFormat
	 * @return true or false
	 */
	public static boolean isTimeLegal(String strDate) {
		SimpleDateFormat sdf = new SimpleDateFormat(yyyy_MM_dd_HH_mm_ss);
		try {
			sdf.parse(strDate);
		} catch (ParseException e) {
			System.err.println("-------------------strDate=" + strDate);
			return false;
		}
		return true;
	}

	/**
	 * 得到当前时间 type 可传入例如yyyy-MM-dd
	 * 
	 * @return String
	 */
	public static String getCurrentDate(String type) {
		return getFormatDate(new Date(), type);
	}

	/**
	 * 得到当前时间 年月日时分称 （yyyyMMddHHmmss）
	 * 
	 * @return String
	 */
	public static String getCurrentTime() {
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat(yyyyMMddHHmmss);
		return sdf.format(date);
	}

	/**
	 * 根据开始时间和结束时间返回时间段内的时间集合
	 * 
	 * @param beginDate
	 * @param endDate
	 * @return List
	 */
	@SuppressWarnings("unchecked")
	public static List getDatesBetweenTwoDate(Date beginDate, Date endDate) {
		List lDate = new ArrayList();
		lDate.add(beginDate);// 把开始时间加入集合
		Calendar cal = Calendar.getInstance();
		// 使用给定的 Date 设置此 Calendar 的时间
		cal.setTime(beginDate);
		boolean bContinue = true;
		while (bContinue) {
			// 根据日历的规则，为给定的日历字段添加或减去指定的时间量
			cal.add(Calendar.DAY_OF_MONTH, 1);
			// 测试此日期是否在指定日期之后
			if (endDate.after(cal.getTime())) {
				lDate.add(cal.getTime());
			} else {
				break;
			}
		}
		lDate.add(endDate);// 把结束时间加入集合
		return lDate;
	}

	public static String getYearMonthByTimeStamp(long timeSecond) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeSecond * 1000);
		String yearStr = String.valueOf(calendar.get(Calendar.YEAR));
		String monthStr = null;
		int month = calendar.get(Calendar.MONTH) + 1;
		if (month < 10) {
			monthStr = "0" + month;
		} else {
			monthStr = String.valueOf(month);
		}
		return yearStr + monthStr;
	}

	/**
	 * 时间戳转化成年月日字符串（20150505）
	 * 
	 * @param timeSecond
	 *            long型时间戳
	 * @return 年月日字符串
	 */
	public static String getYearMonthDayByTimeStamp(long timeSecond) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeSecond * 1000);
		String yearStr = String.valueOf(calendar.get(Calendar.YEAR));
		String monthStr = null;
		String dayStr = null;
		int month = calendar.get(Calendar.MONTH) + 1;
		if (month < 10) {
			monthStr = "0" + month;
		} else {
			monthStr = String.valueOf(month);
		}
		int day = calendar.get(Calendar.DATE);
		if (day < 10) {
			dayStr = "0" + day;
		} else {
			dayStr = String.valueOf(day);
		}
		return yearStr + monthStr + dayStr;
	}

	/**
	 * 时间戳转化成年月日小时字符串（2015050510）
	 * 
	 * @param timeSecond
	 *            long型时间戳
	 * @return 年月日时字符串
	 */
	public static String getYearToHourTimeStamp(long timeSecond) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTimeInMillis(timeSecond * 1000);
		String yearStr = String.valueOf(calendar.get(Calendar.YEAR));
		String monthStr = null;
		String dayStr = null;
		String hourStr = null;
		int month = calendar.get(Calendar.MONTH) + 1;
		if (month < 10) {
			monthStr = "0" + month;
		} else {
			monthStr = String.valueOf(month);
		}
		int day = calendar.get(Calendar.DATE);
		if (day < 10) {
			dayStr = "0" + day;
		} else {
			dayStr = String.valueOf(day);
		}
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		if (hour < 10) {
			hourStr = "0" + hour;
		} else {
			hourStr = String.valueOf(hour);
		}

		return yearStr + monthStr + dayStr + hourStr;
	}

	public static StringBuffer getListDay(String strBeginDate, String strEndDate) {
		// Date date1 = strToDate("20150601101000");
		// Date date2 = strToDate("201506011520000");
		Date date1 = strToDate(strBeginDate.substring(0, 8));
		Date date2 = strToDate(strEndDate.substring(0, 8));
		List listDate = new ArrayList();
		listDate = getDatesBetweenTwoDate(date1, date2);
		List lDate = new ArrayList(new HashSet(listDate));// 去重
		StringBuffer dateStr = new StringBuffer();
		dateStr.append("('");
		for (int i = 0; i < lDate.size(); i++) {
			dateStr.append((getFormatDate((Date) lDate.get(i), "dd")) + "'");
			if (i < lDate.size() - 1) {
				dateStr.append("," + "'");
			}
		}

		return dateStr.append(")");
	}

	public static String timestampToDateStr(long time, String timeFormat) {
		time = time * 1000;
		Date date = new Date(time);
		return getFormatDate(date, timeFormat);
	}

	/**
	 * 字符串的日期格式的计算
	 */
	public static int daysBetween(String smdate, String bdate) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		Calendar cal = Calendar.getInstance();
		cal.setTime(sdf.parse(smdate));
		long time1 = cal.getTimeInMillis();
		cal.setTime(sdf.parse(bdate));
		long time2 = cal.getTimeInMillis();
		long between_days = (time2 - time1) / (1000 * 3600 * 24);

		return Integer.parseInt(String.valueOf(between_days));
	}

	public static String parseTime(String time) {
		if (time.length() >= 14) {
			String year = time.substring(0, 4);
			String month = time.substring(4, 6);
			String day = time.substring(6, 8);
			String hour = time.substring(8, 10);
			String minute = time.substring(10, 12);
			String seconds = time.substring(12, 14);
			// yyyy-MM-dd HH:mm:ss
			return year + "-" + month + "-" + day + " " + hour + ":" + minute + ":" + seconds;
		} else {
			return null;
		}
	}

	public static void main(String[] args) throws Exception {
		// yyyy-MM-dd HH:mm:ss
		/*
		 * Date date1 = strToDate( ("20150630120000").substring(0, 8)); Date
		 * date2 = strToDate(("20150708180000").substring(0, 8)); List listDate
		 * = new ArrayList(); listDate = getDatesBetweenTwoDate(date1,date2);
		 * List lDate = new ArrayList(new HashSet(listDate));//去重 StringBuffer
		 * dateStr = new StringBuffer(); dateStr.append("'"); for(int i=0; i<
		 * lDate.size();i++){
		 * dateStr.append((getFormatDate((Date)lDate.get(i),"dd"))+"'"); if(i <
		 * lDate.size()-1){ dateStr.append(","+"'"); } }
		 */

		System.out.println(impalaStrToDateLong("20150602150810"));

		// System.out.println(strToDate("2014-08-11 14:38:29.0",
		// yyyy_MM_dd_HH_mm_ss));
		// System.out.println(getFormatDate(strToDate("2015-08-11 14:38:29.0",
		// yyyy_MM_dd_HH_mm_ss), yyyy_MM_dd_HH_mm_ss));

		// System.out.println(timestampToDateStr(1416240001,
		// yyyy_MM_dd_HH_mm_ss));
		// System.out.println(getYearMonthByTimeStamp(1416240001));

		// System.out.println(epoch);

	}
}
