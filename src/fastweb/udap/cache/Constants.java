package fastweb.udap.cache;


import fastweb.udap.util.PropertiesConfig;

/**
 * 名称: Constants.java
 * 描述: 常量定义
 * 最近修改时间: Oct 10, 201410:31:42 AM
 * @since Oct 10, 2014
 * @author zhangyi
 */
public class Constants {
	
    public static final String CURRENT_WORK_PATH = PropertiesConfig.getSystemPath();
	public static final String RESPONSE_CODE_PATH = CURRENT_WORK_PATH + "/resp_code.txt";
	/* es config */
	public static final String ES_METRIC = CURRENT_WORK_PATH + "/es_metric.txt";
	public static final String METRIC_TAG = CURRENT_WORK_PATH + "/metric_tag.txt";
	
	public static final String SEARCH_AGGS_SUM = CURRENT_WORK_PATH + "/search_aggs_sum.txt";
	
	static {
		System.out.println("user.dir=" + System.getProperty("user.dir"));
		System.out.println("current_work_path systemPath=" + CURRENT_WORK_PATH);
	}
}
