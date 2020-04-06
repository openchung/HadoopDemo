package top10;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Classname: VideoInfoTop10Mapper
 * @Author: Danniel
 * @Date: 2020/4/25 7:51 下午
 * @Version: 1.0
 * @Description: 實現自定義map類，在這裡實現核心欄位的拼裝
 **/
public class VideoInfoTop10Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        //讀取清洗之後的每一行數據
        String[] fields = v1.toString().split("\t");
        String uid = fields[0];
        long length = Long.parseLong(fields[4]);
        // 組裝k2 v2
//        Text k2 = new Text();
//        k2.set(uid);
//        LongWritable v2 = new LongWritable();
//        v2.set(length);
        context.write(new Text(uid), new LongWritable(length));
    }
}
