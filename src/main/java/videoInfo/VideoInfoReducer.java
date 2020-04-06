package videoInfo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VideoInfoReducer extends Reducer<Text, VideoInfoWritable, Text, VideoInfoWritable> {
    @Override
    protected void reduce(Text k2, Iterable<VideoInfoWritable> v2s, Context context) throws IOException, InterruptedException {
        long goldSum = 0;
        long watchnumpvSum = 0;
        long followerSum = 0;
        long lengthSum = 0;
        //從v2s中把相同Key的value取出來，進行累加求和
        for (VideoInfoWritable v2 : v2s) {
            goldSum += v2.getGold();
            watchnumpvSum += v2.getWatchnumpv();
            followerSum += v2.getFollower();
            lengthSum += v2.getLength();
        }

        //組裝k3,v3
        Text k3 = k2;
        VideoInfoWritable v3 = new VideoInfoWritable(goldSum, watchnumpvSum, followerSum, lengthSum);
        context.write(k3, v3);
    }
}
