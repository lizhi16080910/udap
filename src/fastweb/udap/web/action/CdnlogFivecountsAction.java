package fastweb.udap.web.action;

import java.util.Arrays;
import java.util.Map;

import org.apache.struts2.ServletActionContext;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.GetDomainCsFromCounter;

/**
 * 
 * @ClassName: CdnlogFivecountsAction
 * @author LiuMing
 * @date 2016年11月19日 上午09:50:00
 * @Description: TODO(5分钟计数器带宽)
 */
@Namespace("/")
@Action(value = "cdnlogfivecountsaction")
public class CdnlogFivecountsAction extends ActionSupport {

    private static final long serialVersionUID = 1592533774413104507L;

    private String domain;
    private String day;

    String result = null;

    @Override
    public String execute() throws Exception {
        Map<Long, Long> resultmap = GetDomainCsFromCounter.getDomainFiveMinuteCs(domain, day);
        Object[] key = resultmap.keySet().toArray();
        Arrays.sort(key);
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < key.length; i++) {
            sb.append(key[i] + " " + resultmap.get(key[i]) + "\n");
        }
        this.result = sb.toString();
        ServletActionContext.getResponse().getWriter().write(result);
        return null;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public void setDay(String day) {
        this.day = day;
    }

}
