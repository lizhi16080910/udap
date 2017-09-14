package fastweb.udap.bean;


import java.util.Map;



/**
 * 名称: RespQueryResult.java
 * 描述: 
 * 最近修改时间: Nov 18, 201411:13:06 AM
 * @since Nov 18, 2014
 * @author zhangyi
 */
public class RespQueryResult {
    private String metric;
    private Tag tags;
    private Map<Long,Long> dps;
    
    public String getMetric() {
        return metric;
    }
    
    public void setMetric(String metric) {
        this.metric = metric;
    }

    public Map<Long, Long> getDps() {
        return dps;
    }
    
    public void setDps(Map<Long, Long> dps) {
        this.dps = dps;
    }

    public Tag getTags() {
        return tags;
    }

    public void setTags(Tag tags) {
        this.tags = tags;
    }
}



