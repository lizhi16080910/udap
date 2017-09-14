/** 
 * @Title: FileUtil.java 
 * @Package com.fastweb.cdnlog.bandcheck.util 
 * @author LiFuqiang 
 * @date 2016年7月13日 下午3:08:55 
 * @version V1.0 
 * @Description: TODO(用一句话描述该文件做什么) 
 * Update Logs: 
 * **************************************************** * Name: * Date: * Description: ****************************************************** 
 */
package fastweb.udap.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @ClassName: FileUtil
 * @author LiFuqiang
 * @date 2016年7月13日 下午3:08:55
 * @Description: TODO(这里用一句话描述这个类的作用)
 */
public class FileUtil {
    private static final Log LOG = LogFactory.getLog(FileUtil.class);

    /**
     * @Title: main
     * @param args
     * @throws
     * @Description: TODO(这里用一句话描述这个方法的作用)
     */

    public static void main(String[] args) {
        System.out.println(ifHdfsFileExist("/user/cdnlog_filter/2016/08/24/14/19"));
    }

    public static void listToFile(File file, List<String> list, String charSet) {
        // File file = new File(path);

        File parentDir = new File(file.getAbsolutePath()).getParentFile();
        if (!parentDir.exists()) {
            parentDir.mkdirs();
        }
        Writer writer = null;
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            writer = new OutputStreamWriter(fos, charSet);

            for (String str : list) {
                writer.write(str);
                writer.write("\n");
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (writer != null) {
                    writer.close();
                }
                if (fos != null) {
                    fos.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }
    }

    public static void listToFile(String path, List<String> list, String charSet) {
        listToFile(new File(path), list, charSet);
    }

    public static List<String> readFile(String path, String charSet) {
        return readFile(new File(path), charSet);
    }

    public static List<String> readFile(File file, String charSet) {
        List<String> list = new ArrayList<String>();
        // String encode = getFileEncode(path);
        // System.out.println(encode);
        FileInputStream fInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader buffreader = null;
        try {
            fInputStream = new FileInputStream(file);
            inputStreamReader = new InputStreamReader(fInputStream, charSet);
            buffreader = new BufferedReader(inputStreamReader);
            String line = null;
            while ((line = buffreader.readLine()) != null) {
                list.add(line);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (fInputStream != null) {
                    fInputStream.close();
                }
                if (inputStreamReader != null) {
                    inputStreamReader.close();
                }
                if (buffreader != null) {
                    buffreader.close();
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }

        }
        return list;
    }

    public static List<String> readFile(File file) {
        return readFile(file, "utf8");
    }

    public static List<String> readFile(String path) {
        return readFile(path, "utf8");
    }

    public static void listToFile(String path, List<String> list) {
        listToFile(path, list, "utf8");
    }

    public static List<String> readHdfsFile(String path) {
        List<String> list = new ArrayList<String>();
        FileSystem fs = null;
        FSDataInputStream inputStream = null;
        BufferedReader reader = null;
        Configuration conf = new Configuration();
        try {
            fs = FileSystem.get(conf);
            inputStream = fs.open(new Path(path));
            reader = new BufferedReader(new InputStreamReader(inputStream));
            String line = null;
            while ((line = reader.readLine()) != null) {
                list.add(line);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (reader != null) {
                    reader.close();
                }
                if (inputStream != null) {
                    inputStream.close();
                }
                if (fs != null) {
                    fs.close();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        return list;
    }

    public static List<String> readDomains(String path) throws IOException {
        List<String> domains = new ArrayList<String>();
        FileReader fileReader = new FileReader(new File(path));
        BufferedReader bufReader = new BufferedReader(fileReader);
        String line = null;
        while ((line = bufReader.readLine()) != null) {
            domains.add(line.trim());
        }
        bufReader.close();
        fileReader.close();
        return domains;
    }

    public static boolean ifHdfsFileExist(String path) {
        return ifHdfsFileExist(new Path(path));
    }

    public static boolean ifHdfsFileExist(Path path) {
        Configuration conf = new Configuration();
        // conf.set("fs.default.name", "hdfs://192.168.100.202:8020");
        FileSystem fs = null;
        boolean ifExist = false;
        try {
            fs = FileSystem.get(conf);
            if (fs.exists(path)) {
                ifExist = true;
            }
        }
        catch (IOException e) {
            LOG.error(e.getMessage());
            ifExist = false;
        }
        finally {
            if (fs != null) {
                try {
                    fs.close();
                }
                catch (IOException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
        return ifExist;
    }

}
