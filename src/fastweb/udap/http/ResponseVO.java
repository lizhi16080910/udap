package fastweb.udap.http;

/**
 * 名称: ResponseVO.java
 * 描述: http response返回结果封装
 * 最近修改时间: Oct 10, 201410:33:11 AM
 * @since Oct 10, 2014
 * @author zhangyi
 */
public class ResponseVO {

	private String uri;
	private String contentType;
	private int statusCode;
	private String respContent;
	
	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getContentType() {
		return contentType;
	}

	public void setContentType(String contentType) {
		this.contentType = contentType;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public String getRespContent() {
		return respContent;
	}

	public void setRespContent(String respContent) {
		this.respContent = respContent;
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		sb.append("uri=");
		sb.append(getUri());
		sb.append("\nstatusCode=");
		sb.append(getStatusCode());
		sb.append("\ncontentType=");
		sb.append(getContentType());
		return sb.toString();
	}
}


