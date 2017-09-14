package fastweb.udap.util;

import java.io.FileInputStream;
import java.util.Properties;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import fastweb.udap.bean.RespQueryResult;

/**
 * 名称: JsonToRequestVo.java
 * 
 * @since Oct 14, 2014
 * @author zhangyi
 */
public class JsonUtil {
    
    private static final ObjectMapper mapper = new ObjectMapper();  
    
    private JsonUtil() {
    }
    
    public static ObjectMapper getInstance() {    
        return mapper;    
    } 

    public static <T> T jsonToObject(String json, Class<T> pojo) throws Exception {
        return mapper.readValue(json, pojo);
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T jacksonToCollection(String src, Class<?> collectionClass, Class<?>... valueType) throws Exception {
        JavaType javaType = mapper.getTypeFactory().constructParametricType(collectionClass, valueType);
        return (T) mapper.readValue(src, javaType);
    }
    
    public static String objectToJson(Object bean) throws Exception {
        String json = mapper.writeValueAsString(bean);    //方法1 官方api
//      String  json =  mapper.writerWithDefaultPrettyPrinter().writeValueAsString(bean);   //方法2 美化json
        
//        StringWriter sw = new StringWriter();    //方法3 来自论坛
//        JsonGenerator gen = new JsonFactory().createJsonGenerator(sw);
//        mapper.writeValue(gen, bean);
//        gen.close();
//        String json =  sw.toString();
        return json;
    }

    public static void main(String[] args) throws Exception {
        //1 测试jsonToBean
//        Properties properties = new Properties();
//        properties.load(new FileInputStream("D:\\workspaces_myeclipse6.5\\udap\\test_json.txt"));
//        String jsonQuery = (String) properties.get("jsonQuery1");
//        System.out.println("jsonString=" + jsonQuery);
//        TrafficRequestBean bean = jsonToBean(jsonQuery, TrafficRequestBean.class);
//        System.out.println(bean.getStart());
//        System.out.println(bean.getQuerys().get(0).getTags().get("domain"));
        
        //1-1 测试jsonToBean数组
        Properties properties = new Properties();
        properties.load(new FileInputStream("D:\\workspaces_myeclipse6.5\\udap\\test_tsdb_json.txt"));
        String jsonQuery = (String) properties.get("jsonQuery1");
        System.out.println("jsonString=" + jsonQuery);
        RespQueryResult info = jsonToObject(jsonQuery, RespQueryResult.class);
        System.out.println(info.getDps().get("123"));
    }
}


