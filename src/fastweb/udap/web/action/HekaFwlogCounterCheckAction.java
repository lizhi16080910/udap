package fastweb.udap.web.action;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.struts2.ServletActionContext;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Namespace;
import org.apache.struts2.convention.annotation.Result;

import com.opensymphony.xwork2.ActionSupport;

import fastweb.udap.util.CheckUtil;

/**
 * 
 * @ClassName: CdnlogErrorStatusQueryAction
 * @author LiFuqiang
 * @date 2016年3月15日 上午10:04:00
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
@Namespace("/")
@Action(value = "hekafwlogcountercheckaction", results = {
        @Result(name = ActionSupport.SUCCESS, type = "json", params = { "excludeProperties",
                "message" }),
        @Result(name = ActionSupport.ERROR, type = "json", params = { "includeProperties",
                "message" }) })
public class HekaFwlogCounterCheckAction extends ActionSupport {
    private final Log log = LogFactory.getLog(getClass());
    private static final long serialVersionUID = 1592533774413104507L;

    private String reslt;
    /* 错误信息 */
    private String message;

    private HttpServletRequest request = null;

    @Override
    public String execute() throws Exception {
        HttpServletRequest request = ServletActionContext.getRequest();

        String dayP = request.getParameter("d12");
        String[] temp = dayP.split("-");
        String month = null;
        String day = null;
        if (temp[1].length() == 1) {
            month = "0" + temp[1];
        }
        else {
            month = temp[1];
        }

        if (temp[2].length() == 1) {
            day = "0" + temp[2];
        }
        else {
            day = temp[2];
        }
        day = temp[0] + "-" + month + "-" + day;
        log.info(day);
        CheckUtil check = new CheckUtil("dl.vip.yy.com", day.replace("-", "/"), "");
        String fileName = "dl.vip.yy.com-" + day + "-" + System.currentTimeMillis() + ".csv";
        HttpServletResponse response = ServletActionContext.getResponse();
        // 取得输出流
        OutputStream out = response.getOutputStream();
        // 清空输出流
        response.reset();

        // 设置响应头和下载保存的文件名
        response.setHeader("content-disposition", "attachment;filename=" + fileName);
        // 定义输出类型
        response.setContentType("APPLICATION/msexcel");
        // response.setContentType("text/plain");
        response.setCharacterEncoding("GBK");
        out.write(readInputStream(check.getResult()));
        // out.write(readInputStream(check.getResult()));
        out.close();

        // 这一行非常关键，否则在实际中有可能出现莫名其妙的问题！！！
        response.flushBuffer();// 强行将响应缓存中的内容发送到目的地

        return null;
    }

    /**
     * 从输入流获取数据
     * 
     * @param inputStream
     * @return
     * @throws Exception
     */
    public static byte[] readInputStream(InputStream inputStream) throws Exception {
        byte[] buffer = new byte[1024];
        int len = -1;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        while ((len = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, len);
        }
        outputStream.close();
        inputStream.close();
        return outputStream.toByteArray();
    }

    /**
     * 从输入流获取数据
     * 
     * @param inputStream
     * @return
     * @throws Exception
     */
    public static byte[] readInputStream(List<String> strs) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (String str : strs) {
            outputStream.write(str.getBytes());
            outputStream.write("\n".getBytes());
        }
        outputStream.close();
        return outputStream.toByteArray();
    }

    /**
     * 从输入流获取数据
     * 
     * @param inputStream
     * @return
     * @throws Exception
     */
    public static byte[] readInputStream(List<String> strs, String charset) throws Exception {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (String str : strs) {
            outputStream.write(str.getBytes());
            outputStream.write("\n".getBytes());
        }
        outputStream.close();
        return outputStream.toByteArray();
    }
}
