package fastweb.udap.http;

import fastweb.udap.bean.Tag;

/**
 * 名称: RequestVO.java
 * 描述: http request请求参数封装
 * 最近修改时间: Oct 10, 201410:32:42 AM
 * @since Oct 10, 2014
 * @author zhangyi
 */
public class RequestVO {

	private String metric;
	private String start;
	private String end;
	private Tag tag;
	private String queryString;
	
	public RequestVO(){
	}

	public RequestVO(String metric, String start, String end, Tag tag) {
	    super();
	    this.metric = metric;
	    this.start = start;
	    this.end = end;
	    this.tag = tag;
	}

	public String getMetric() {
		return metric;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public String getStart() {
		return start;
	}

	public void setStart(String start) {
		this.start = start;
	}

	public String getEnd() {
		return end;
	}

	public void setEnd(String end) {
		this.end = end;
	}

    public Tag getTag() {
        return tag;
    }

    public void setTag(Tag tag) {
        this.tag = tag;
    }

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }
}


