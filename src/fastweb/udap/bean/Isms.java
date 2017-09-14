package fastweb.udap.bean;

public class Isms {

	/* 开始时间 */
	private long begin = 0;
	/* 结束时间，时间跨度不超过两小时 */
	private long end = 0;
	/* 触发的规则ID，不做查询，只为携带给接口 */
	private long id = 0;
	/* 规则类型（监控规则 和 过滤规则） */
	private String type;
	/* 节点IP起始段位 */
	private String nodeIpStart;
	/* 节点IP结束段位 */
	private String nodeIpEnd;
	/* 查询URL */
	private String url;
	/* 用户IP开始段位 */
	private String userIpStart;
	/* 用户IP结束段位 */
	private String userIpEnd;
	/* 查询域名 */
	private String domain;
	/* 请求唯一标识 */
	private String commandId;
	/*put推送http地址*/
	private String httpPutUrl;
	/*一个文件条数*/
	private int limit = 100000;
	/*一次request 唯一UUID--时间标识,作为判断一次请求是否在队列积压过长*/
	private long uuid;
	
	public long getBegin() {
		return begin;
	}
	public void setBegin(long begin) {
		this.begin = begin;
	}
	public long getEnd() {
		return end;
	}
	public void setEnd(long end) {
		this.end = end;
	}
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getNodeIpStart() {
		return nodeIpStart;
	}
	public void setNodeIpStart(String nodeIpStart) {
		this.nodeIpStart = nodeIpStart;
	}
	public String getNodeIpEnd() {
		return nodeIpEnd;
	}
	public void setNodeIpEnd(String nodeIpEnd) {
		this.nodeIpEnd = nodeIpEnd;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getUserIpStart() {
		return userIpStart;
	}
	public void setUserIpStart(String userIpStart) {
		this.userIpStart = userIpStart;
	}
	public String getUserIpEnd() {
		return userIpEnd;
	}
	public void setUserIpEnd(String userIpEnd) {
		this.userIpEnd = userIpEnd;
	}
	public String getDomain() {
		return domain;
	}
	public void setDomain(String domain) {
		this.domain = domain;
	}
	public String getCommandId() {
		return commandId;
	}
	public void setCommandId(String commandId) {
		this.commandId = commandId;
	}
	public String getHttpPutUrl() {
		return httpPutUrl;
	}
	public void setHttpPutUrl(String httpPutUrl) {
		this.httpPutUrl = httpPutUrl;
	}
	public int getLimit() {
		return limit;
	}
	public void setLimit(int limit) {
		this.limit = limit;
	}
	public long getUuid() {
		return uuid;
	}
	public void setUuid(long uuid) {
		this.uuid = uuid;
	}
}
