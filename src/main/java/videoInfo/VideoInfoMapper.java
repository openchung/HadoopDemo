package videoInfo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 實現自定義Map類，在這裡實現核心欄位的拼裝
 *
 * */
public class VideoInfoMapper extends Mapper <LongWritable, Text, Text, VideoInfoWritable> {
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        //讀取清洗之後的每一行數據
        String line = v1.toString();
        //使用制表符號對數據進行切割
        String[] fields = line.split("\t");

        String uid = fields[0];
        long gold = Long.parseLong(fields[1]);
        long watchnumpv = Long.parseLong(fields[2]);
        long follower = Long.parseLong(fields[3]);
        long length = Long.parseLong(fields[4]);

        //組裝k2 v2
//        Text k2 = new Text();
//        k2.set(uid);

        VideoInfoWritable v2 = new VideoInfoWritable(gold, watchnumpv, follower, length);
        context.write(new Text(uid), v2);
    }
}
