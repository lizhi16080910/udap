package fastweb.udap.cache;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import fastweb.udap.util.FileHelper;
import fastweb.udap.util.StringUtil;


/**
 * 名称: CacheLoader.java
 * 描述: 缓存加载与初始化
 * 最近修改时间: Oct 10, 201410:31:00 AM
 * @since Oct 10, 2014
 * @author zhangyi
 */
public class CacheLoader {

	private static final long serialVersionUID = 1L;
	private static final Log log = LogFactory.getLog(CacheLoader.class);
	private static final Map<String,String> respCodeMap = new HashMap<String,String>();
	private static final Map<String,String> metricMap = new HashMap<String,String>();
	private static final Map<String,Map<String,String>> metricTagMap = new HashMap<String,Map<String,String>>();
	
	static {
		initFileCache();
	}
	
	/**
	 * 初始化文件缓存
	 */
	public static void initFileCache() {
		log.info("config file path:" + Constants.CURRENT_WORK_PATH);
		List<String> respCodeList = FileHelper.readerStream(Constants.RESPONSE_CODE_PATH);
		for (String str : respCodeList) {
			if (!StringUtil.isEmpty(str)) {
				try {
					String code = str.substring(0, str.lastIndexOf("|")).trim();
					String message = str.substring(str.lastIndexOf("|") + 1).trim();
					if (StringUtil.isEmpty(code) || StringUtil.isEmpty(message)) {
						continue;
					} else {
						respCodeMap.put(code, message);
					}
				} catch (Exception e) {
					log.error("Exception,file content format is not correct,str=" + str);
				}
			}		
		}
		
        List<String> metricList = FileHelper.readerStream(Constants.ES_METRIC);
        for (String str : metricList) {
            if (!StringUtil.isEmpty(str)) {
                try {
                    String metric = str.substring(0, str.lastIndexOf("=")).trim();
                    String code = str.substring(str.lastIndexOf("=") + 1).trim();
                    if (StringUtil.isEmpty(metric) || StringUtil.isEmpty(code)) {
                        continue;
                    } else {
                        metricMap.put(metric, code);
                    }
                } catch (Exception e) {
                    log.error("Exception,file content format is not correct,str=" + str);
                }
            }       
        }
        
        List<String> metricTagList = FileHelper.readerStream(Constants.METRIC_TAG);
        for (String str : metricTagList) {
            if (!StringUtil.isEmpty(str)) {
                try {
                    String metric = str.substring(0, str.lastIndexOf("=")).trim();
                    String tagStr = str.substring(str.lastIndexOf("=") + 1).trim();
                    if (StringUtil.isEmpty(metric) || StringUtil.isEmpty(tagStr)) {
                        continue;
                    } else {
                        String[] tags = tagStr.split(",");
                        Map<String,String> tagMap = new HashMap<String,String>(); 
                        for (int i=0; i<tags.length; i++) {
                            tagMap.put(tags[i], tags[i]);
                        }
                        metricTagMap.put(metric, tagMap);
                    }
                } catch (Exception e) {
                    log.error("Exception,file content format is not correct,str=" + str);
                }
            }       
        }
		
        log.info("metricMap=" + metricMap);
        log.info("metricTagMap=" + metricTagMap);
		log.info("respCodeMap=" + respCodeMap);
	}
	
	public static Map<String, String> getRespCodeMap() {
		return respCodeMap;
	}

	public static Map<String, String> getMetricMap() {
        return metricMap;
    }
	
    public static Map<String, Map<String, String>> getMetricTagMap() {
        return metricTagMap;
    }

    public static void main(String[] args) {
		new CacheLoader();
	}
}

