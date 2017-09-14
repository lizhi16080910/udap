/** 
 * @Title: UrlUtil.java 
 * @Package com.fastweb.cdnlog.bandcheck.util 
 * @author LiFuqiang 
 * @date 2016年8月8日 上午11:39:30 
 * @version V1.0 
 * @Description: TODO(用一句话描述该文件做什么) 
 * Update Logs: 
 * **************************************************** * Name: * Date: * Description: ****************************************************** 
 */
package fastweb.udap.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @ClassName: UrlUtil
 * @author LiFuqiang
 * @date 2016年8月8日 上午11:39:30
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class UrlUtil {
    private static final Log LOG = LogFactory.getLog(UrlUtil.class);

    /**
     * @Title: main
     * @param args
     * @throws
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */

    public static void main(String[] args) {

        // TODO Auto-generated method stub

    }

    /**
     * 向指定 URL 发送POST方法的请求
     * 
     * @param posturl
     *            请求URL
     * @param postbody
     *            请求参数，请求参数应该是 name1=value1&name2=value2 的形式
     * @return 所代表远程资源的响应结果
     */
    public static String postData(String postUrl, String postBody) {
        PrintWriter out = null;
        BufferedReader in = null;
        String result = "";
        try {
            URL realUrl = new URL(postUrl);
            // 打开和URL之间的连接
            URLConnection conn = realUrl.openConnection();
            // 设置通用的请求属性
            conn.setRequestProperty("accept", "*/*");
            conn.setRequestProperty("connection", "Keep-Alive");
            conn.setRequestProperty("user-agent",
                    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)");
            // 发送POST请求必须设置如下两行
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // 获取URLConnection对象对应的输出流
            out = new PrintWriter(conn.getOutputStream());
            // 发送请求参数
            out.print(postBody);
            // flush输出流的缓冲
            out.flush();
            // 定义BufferedReader输入流来读取URL的响应
            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        }
        catch (Exception ex) {
            LOG.info("发送 POST 请求出现异常！" + ex);
            ex.printStackTrace();
        }
        // 使用finally块来关闭输出流、输入流
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (in != null) {
                    in.close();
                }
            }
            catch (IOException ex) {
                LOG.info("流关闭出现异常！" + ex);
            }
        }
        return result;
    }
}
