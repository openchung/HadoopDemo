package top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import utils.MapUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Classname: VideoInfoTopReducer
 * @Author: Ming
 * @Date: 2020/1/27 7:53 下午
 * @Version: 1.0
 * @Description: TODO
 **/
public class VideoInfoTop10Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    //存儲主播id和直播時長
    private Map<String, Long> info = new HashMap<>();

    //任務初始化的時候執行一次，且僅執行一次，一般在裡面做一些初始化資源鏈接的動作
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context) throws IOException, InterruptedException {
        long lengthSum = 0;
        for (LongWritable v2 : v2s) {
            lengthSum += v2.get();
        }

        info.put(k2.toString(), lengthSum);
    }

    /**
     * 任務結束的時候執行一次，僅執行一次，做一些關閉資源的操作
     * */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        //從配置類中取出dt參數(日期時間)
        String dt = conf.get("dt");

        //根據Map中的value排序
        Map<String, Long> sortedMap = MapUtils.sortValue(info);
        //初始值為1，循環10次，取出前10的主播id和開播時長寫到輸出文件中。
//        Set<Map.Entry<String,Long>> entries = sortedMap.entrySet();
//        Iterator<Map.Entry<String,Long>> it = entries.iterator();
//        int count = 1;
//        while(count<=10 && it.hasNext()) {
//            Map.Entry<String, Long> entry = it.next();
//            String key = entry.getKey();
//            Long value = entry.getValue();
//            //封裝k3,v3
//            Text k3 = new Text();
//            k3.set(dt+"\t"+key);
//            LongWritable v3 = new LongWritable();
//            v3.set(value);
//            context.write(k3,v3);
//            count++;
//        }
        AtomicInteger count = new AtomicInteger(1);
        sortedMap.forEach((key, val) -> this.writeResult(key, val, dt, context, count));
    }

    private void writeResult(String key, Long val, String dt, Context context, AtomicInteger count) {
        try {
            if (count.get() > 10) {
                return;
            }
            context.write(new Text(dt + "\t" + key), new LongWritable(val));
        } catch (Exception e) {
            e.printStackTrace();
        }
        count.getAndIncrement();
    }
}