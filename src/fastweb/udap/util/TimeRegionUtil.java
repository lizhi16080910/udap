package fastweb.udap.util;

import java.util.ArrayList;
import java.util.List;


/**
 * 名称: TimeRegionUtil.java
 * 描述: 
 * 最近修改时间: Oct 24, 201410:36:53 AM
 * @since Oct 24, 2014
 * @author zhangyi
 */
public class TimeRegionUtil {
    
    private static final long ONE_HOUR_SECOND = 60 * 60;
    
    public static List<Long> getTimeRegion(String startTimeStr, String endTimeStr, String timeFormatStr) {
        long startTime = DatetimeUtil.strToDate(startTimeStr, timeFormatStr).getTime() / 1000;
        long endTime = DatetimeUtil.strToDate(endTimeStr, timeFormatStr).getTime() / 1000;
        return getTimeRegion(startTime, endTime);
    }
    
    public static List<Long> getTimeRegion(long startTime, long endTime) {
        List<Long> result = new ArrayList<Long>();
        long timePoint;
        long n = startTime % ONE_HOUR_SECOND;
        if (n == 0) {
            timePoint = startTime;
        } else {
            startTime = startTime - n;
            timePoint = startTime + ONE_HOUR_SECOND;
        }
        while (true) {
            if (timePoint >= startTime && timePoint <= endTime) {
                result.add(timePoint);
            } else {
                break;
            }
            timePoint = timePoint + ONE_HOUR_SECOND;
        }
        return result;
    }
    
    public static void main(String[] args) {
        //yyyy-MM-dd HH:mm:ss
//        System.out.println(getTimeRegion(1412989200, 1412992800));
//        List<Long> result = getTimeRegion("2014-09-01 08:00:00", "2014-10-01 07:59:59", DatetimeUtil.yyyy_MM_dd_HH_mm_ss);
//        System.out.println(result.size() + " " + result);
        
        List<Long> result2 = getTimeRegion("2014/10/01-08:04:00", "2014/10/02-07:59:59", "yyyy/MM/dd-HH:mm:ss");
        System.out.println(result2.size() + " " + result2);
    }
}

