package fastweb.udap.http;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import fastweb.udap.util.PropertiesConfig;

public class EsRequestBalance {

    private static final int count = PropertiesConfig.getIntProperty("es.count", 1);
    private static Map<Integer, String>  serverMap = new HashMap<Integer,String>();
    private static final Random r = new Random();
    
    static {
        init();
    }
    
    public static void init() {
        for (int i=1; i<=count; i++) {
            serverMap.put(i, PropertiesConfig.getStringProperty("es.server." + i, "http://192.168.100.205:9200"));
        }
    }
    
    public static String getOneServer() {
        int n = r.nextInt(count) + 1;
        return serverMap.get(n);
    }
    
    public static Map<Integer, String> getFullServerMap() {
        return serverMap;
    }

    public static int getCount() {
        return count;
    }

    public static void main(String[] args) {
        System.out.println(getCount());
        System.out.println(getFullServerMap());
        System.out.println(getOneServer());
    }
}

