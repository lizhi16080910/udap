//package test;
//
//import java.util.List;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.elasticsearch.action.bulk.BulkRequestBuilder;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.indices.IndexAlreadyExistsException;
//
//import fastweb.udap.es.index.ESClient;
//import fastweb.udap.es.index.IndexService;
//import fastweb.udap.es.index.TestData;
//import fastweb.udap.es.mapping.TrafficMapping;
//import fastweb.udap.json.info.Tag;
//import fastweb.udap.util.PropertiesConfig;
//import fastweb.udap.util.TimeRegionUtil;
//
//
//
///**
// * 名称: IndexTrafficThread.java
// * 描述: 
// * 最近修改时间: Nov 3, 20143:13:59 PM
// * @since Nov 3, 2014
// * @author zhangyi
// */
//public class TrafficIndexThread extends Thread {
//    
//    private static final Log log = LogFactory.getLog(TrafficIndexThread.class);
//    private static final ExecutorService pool = Executors.newFixedThreadPool(PropertiesConfig.getIntProperty("visit.http.thread_pool_size", 4));
//    private static final int MAX_BULK_NUMBER = 20000;
//    public static final String index = "cdnlog.traffic";
//    public static String type = "2014";
//    public static final Object LOCK = new Object();
//    public static int threadCount;
//    
//    private boolean isDie = false;
//    private Client client = ESClient.createClusterClient();
//    private BulkRequestBuilder bulkRequest = client.prepareBulk();
//    private String time;
//    
//    public TrafficIndexThread() {
//    }
//    
//    public TrafficIndexThread(String time) {
//        this.time = time;
//    }
//    
//    public static void main(String[] args) throws Exception {
//        //1 查询,建索引
//        new TrafficIndexThread().parallelIndex("2014/11/13-12:00:00", "2014/11/13-15:59:59","yyyy/MM/dd-HH:mm:ss");
//    }
//    
//    
//    /**
//     * 分布式索引
//     * @throws InterruptedException 
//     * @变更记录 Oct 28, 201411:17:46 AM
//     */
//    private void parallelIndex(String start, String end, String timeFormat) throws Exception {
//        long startTime = System.currentTimeMillis();
//        
//        List<Long> timeList = TimeRegionUtil.getTimeRegion(start, end, timeFormat);
//        log.info("timeList.size=" + timeList.size());
//        for (int i = 0; i < timeList.size(); i++) {
//            TrafficIndexThread indexThread = new TrafficIndexThread(String.valueOf(timeList.get(i)));
//            pool.execute(indexThread);
////            threadList.add(indexThread);
//        }
//        while (true) {
//            if (timeList.size() == threadCount) {
//                log.info("------------all thread is end, threadCount=" + threadCount);
//                pool.shutdown();
//                break;
//            }
//            log.info("------------sleep 10s, threadCount=" + threadCount);
//            Thread.sleep(10000);
//        }
//        
//        System.out.println("spend time:" + (System.currentTimeMillis() - startTime) / 1000);
//    }
//    
//    @Override
//    public void run() {
//        log.info("IndexTrafficThread is running............");
//        try {
//            TrafficMapping.buildMapping(client, index, type);            //创建mapping,一个索引库类型一个mapping, 只要存在index就不能再创建mapping。
//        } catch (IndexAlreadyExistsException e1) {
//            log.warn(index + ", index already exists, can not create mapping");
//        } catch (Exception e1) {
//            e1.printStackTrace();
//            return;
//        }
//        
//        try {
//            int count = 0;
//            for (int i=0; i<1000000; i++) {
//                Tag tag = TestData.createTag();
//                TrafficMapping.addIndex(client, bulkRequest, tag, index, type, time, 10000L);
//                count++;
//                if (count > 0 && count % MAX_BULK_NUMBER == 0) {
//                    IndexService.createIndex(client, bulkRequest);
//                    log.info("--------count=" + count);
//                    bulkRequest = client.prepareBulk();   //提交完一次要从新new一次，不然下次提交的是temp的N倍数据
//                }
//            }
//            if (count % MAX_BULK_NUMBER != 0) {
//                IndexService.createIndex(client, bulkRequest);  
//            }
//            log.info("--------------------index total count=" + count);
//        } catch (Exception e) {
//            log.error("IndexTrafficThread Exception! ", e);
//        } finally {
//            log.info(Thread.currentThread().getName() + ", thread is die");
//            isDie = true;
//            client.close();
//            synchronized (LOCK) {
//                threadCount++;
//            }
//        }
//    }
//    
//    public boolean isDie() {
//        return isDie;
//    }
//    
//}
//
//
//
