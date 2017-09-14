/** 
 * @Title: DateUtil.java 
 * @Package com.fastweb.cdnlog.bandcheck.util 
 * @author LiFuqiang 
 * @date 2016年7月13日 下午1:27:38 
 * @version V1.0 
 * @Description: TODO(用一句话描述该文件做什么) 
 * Update Logs: 
 * **************************************************** * Name: * Date: * Description: ****************************************************** 
 */
package fastweb.udap.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * @ClassName: DateUtil
 * @author LiFuqiang
 * @date 2016年7月13日 下午1:27:38
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class DateUtil {

    /**
     * @Title: main
     * @param args
     * @throws ParseException
     * @throws
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */

    public static void main(String[] args) throws Exception {

        System.out.println(getDay(1472573100l));
        // System.out.println(getEndTime("2016/07/24"));
    }

    /**
     * 
     * @Title: get5minutesListByHour
     * @param timeHour
     * @return
     * @throws
     * @Description: 参数以秒为单位，返回值也是以秒为单位
     */
    public static List<Long> get5minutesListByHour(long timeHour) {
        List<Long> list = new ArrayList<Long>();
        for (int i = 1; i < 13; i++) {
            list.add(timeHour + i * 300);
        }
        return list;
    }

    /**
     * 
     * @Title: get5minutesListByHour
     * @param timeHour
     * @return
     * @throws
     * @Description: 参数举例：2016/07/11/10，返回值以秒为单位
     */
    public static List<Long> get5minutesListByHour(String timeHour) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd/HH");
        long l = 0l;
        try {
            l = sdf.parse(timeHour).getTime() / 1000;
        }
        catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
        return get5minutesListByHour(l);
    }

    /**
     * 
     * @Title: get5minutesListByDay
     * @param timeDay
     * @return
     * @throws ParseException
     * @throws
     * @Description: 参数的格式：2016/07/11，返回值以秒为单位
     */
    public static List<Long> get5minutesListByDay(String timeDay) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        long day = sdf.parse(timeDay).getTime() / 1000l;
        return get5minutesListByDay(day);
    }

    /**
     * 
     * @Title: get5minutesListByDay
     * @param timeDay
     * @return
     * @throws ParseException
     * @throws
     * @Description: 参数的格式：2016/07/11，返回值以秒为单位
     */
    public static List<Long> get5minutesListByDay(long day) throws ParseException {
        List<Long> list = new ArrayList<Long>();
        for (int i = 0; i < 24; i++) {
            list.addAll(get5minutesListByHour(day + i * 3600));
        }
        return list;
    }

    /**
     * 
     * @Title: get5minutesListByMonth
     * @param month
     * @return
     * @throws ParseException
     * @throws
     * @Description: 参数的格式：2016/07，返回值以秒为单位
     */
    public static List<Long> get5minutesListByMonth(String timeMonth) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM");
        long month = sdf.parse(timeMonth).getTime() / 1000l;
        long nextMonth = nextMonth(timeMonth);
        List<Long> list = new ArrayList<Long>();
        for (int i = 0; i < 31; i++) {
            long temp = month + i * 24 * 3600;
            if (temp < nextMonth) {
                list.addAll(get5minutesListByDay(temp));
            }
        }
        return list;
    }

    /**
     * 
     * @Title: nextMonth
     * @param timeMonth
     * @return
     * @throws ParseException
     * @throws
     * @Description: 参数的格式：2016/07; 返回值以秒为单位
     */
    public static long nextMonth(String timeMonth) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM");
        long month = sdf.parse(timeMonth).getTime();
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(month);
        c.add(Calendar.MONTH, 1);
        return c.getTimeInMillis() / 1000;
    }

    /**
     * 
     * @Title: nextMonth
     * @param timeMonth
     * @return
     * @throws ParseException
     * @throws
     * @Description: 参数的格式：2016/07; 返回值:2016/08/01
     */
    public static String nextMonthFirstDay(String timeMonth) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        return sdf.format(new Date(nextMonth(timeMonth) * 1000));
    }

    /**
     * 
     * @Title: getToday
     * @return
     * @throws
     * @Description:获取今天的字符串，返回值格式示例：“2016/07/01”
     */
    public static String getToday() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        return sdf.format(new Date());
    }

    /**
     * 
     * @Title: getTodayLong
     * @return
     * @throws
     * @Description: 返回今天起始时间的秒数
     */
    public static long getTodayLong() {
        long current = System.currentTimeMillis() / 1000;
        return current - current % (24 * 3600) - 8 * 3600;
    }

    /***
     * 
     * @Title: nextDay
     * @param day
     * @return
     * @throws ParseException
     * @throws
     * @Description: 获取明天的时间
     * @dd 参数的格式：2016/07/08,返回值：2016/07/09
     */
    public static String nextDay(String day) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        long date = sdf.parse(day).getTime();
        long nextDay = date + 1 * 24 * 3600 * 1000l;
        return sdf.format(new Date(nextDay));
    }

    /**
     * 
     * @Title: getDaysByMonth
     * @param timeMonth
     * @return
     * @throws ParseException
     * @throws
     * @Description: 参数的格式：2016/07； 返回值的格式：2016/07/01,2016/07/02....
     */
    public static List<String> getDaysByMonth(String timeMonth) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        List<String> list = new ArrayList<String>();
        long month = sdf.parse(timeMonth + "/01").getTime() / 1000l;
        long nextMonth = nextMonth(timeMonth);
        for (int i = 0; i < 31; i++) {
            long temp = month + i * 24 * 3600;
            if (temp < nextMonth) {
                list.add(sdf.format(new Date(temp * 1000)));
            }
        }
        return list;
    }

    public static long getStartTime(String timeMonth) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        List<String> list = new ArrayList<String>();
        long month = sdf.parse(timeMonth).getTime() / 1000l;
        // return month - month % (24 * 3600);
        return month;
    }

    /**
     * 
     * @Title: getEndTime
     * @param timeMonth
     * @return
     * @throws ParseException
     * @throws
     * @Description:参数示例：2016/08/01
     */
    public static long getEndTime(String timeMonth) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        List<String> list = new ArrayList<String>();
        long month = sdf.parse(timeMonth).getTime() / 1000l;
        // return month - month % (24 * 3600);
        return month + 24 * 3600 - 1;
    }

    /**
     * 
     * @Title: getDays
     * @param startDay
     * @param endDay
     * @return
     * @throws Exception
     * @throws
     * @Description: 参数及返回值的格式：2016/07/01
     */
    public static List<String> getDays(String startDay, String endDay) throws Exception {
        return getDays(startDay, endDay, "yyyy/MM/dd");
    }

    /**
     * 
     * @Title: getDays
     * @param startDay
     * @param endDay
     * @param sdf
     * @return
     * @throws Exception
     * @throws
     * @Description: 按照sdf的格式要求，返回时间
     */
    public static List<String> getDays(String startDay, String endDay, SimpleDateFormat sdf)
            throws Exception {
        List<String> list = new ArrayList<String>();
        long start = sdf.parse(startDay).getTime() / 1000l;
        long end = sdf.parse(endDay).getTime() / 1000l;
        while (start <= end) {
            String day = sdf.format(new Date(start * 1000));
            list.add(day);
            start += 24 * 3600;
        }
        return list;
    }

    /**
     * 
     * @Title: getDays
     * @param startDay
     * @param endDay
     * @param timeFormat
     * @return
     * @throws Exception
     * @throws
     * @Description: timeFormat的值为“yyyy/MM/dd”，则返回值得形式为“2016/07/01”
     */
    public static List<String> getDays(String startDay, String endDay, String timeFormat)
            throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(timeFormat);
        return getDays(startDay, endDay, sdf);
    }

    /**
     * 
     * @Title: getDay
     * @return
     * @throws
     * @Description:根据time返回“天”，2016/07/01 00:00 归为2016/08/31这一天，每天的开始日期为00:05分
     */
    public static String getDay(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
        return sdf.format(new Date((time - 300) * 1000));
    }
}
