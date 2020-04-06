package top10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import utils.DateUtils;
import videoInfo.VideoInfoJob;
import videoInfo.VideoInfoMapper;
import videoInfo.VideoInfoReducer;
import videoInfo.VideoInfoWritable;

import java.io.IOException;

/**
 * 數據指標統計作業
 * 需求:
 * 2. 統計每天開播時長最長的前10名主播及對應的主播時長
 *
 * 分析:
 * 1. 為了統計每天開播最長的前10名主播資訊，需要在Map階段取得數據中每個主播的id和直播時長
 * 2. 所以map階段的<k2,v2>為<Text, LongWritable>
 * 3. 在reduce端對相同主播的直播時長進行累加求和，把這些數據存儲到一個臨時的map集合中
 * 4. 在reduce端的cleanup函數中對Map集合中的數據根據直播時長進行排序
 * 5. 在cleanup函數中把直播時長最長的前10名主播的資訊寫出到hdfs文件中
 *
 * */
public class VideoInfoTop10Job {
    public static void main(String[] args) throws Exception {
        try {
             if (args.length != 2) {
                 //如果傳參不夠，程式直接跳出
                 System.exit(100);
             }
            // Job需要配置的參數
            Configuration configuration = new Configuration();
            // 從輸入路徑中取得日期
            String[] fields = args[0].split("/");
            String tmpdt = fields[fields.length-1];
            String dt = DateUtils.transDateFormat(tmpdt);
            configuration.set("dt", dt);

            // 創建一個Job
            Job job = Job.getInstance(configuration);
            job.setJarByClass(VideoInfoTop10Job.class);


            // 指定輸入的路徑(可以是文件，也可以是目錄)
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            FileSystem fs = FileSystem.get(configuration);
//        Path outputPath = new Path("video/top");
            Path outputPath = new Path(args[1]);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            //指定輸出路徑(只能指定一個不存在的目錄)
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
//            FileSystem fs = FileSystem.get(configuration);
////        Path outputPath = new Path("video/top");
//            Path outputPath = new Path(args[1]);
//            if (fs.exists(outputPath)) {
//                fs.delete(outputPath, true);
//            }

            //指定Map相關程式碼
            job.setMapperClass(VideoInfoTop10Mapper.class);
            //指定k2的類型
            job.setMapOutputKeyClass(Text.class);
            //指定v2的類型
            job.setMapOutputValueClass(LongWritable.class);

            //指定reduce相關的程式碼
            job.setReducerClass(VideoInfoTop10Reducer.class);
            //指定k3的類型
            job.setOutputKeyClass(Text.class);
            //指定v3的類型
            job.setMapOutputValueClass(LongWritable.class);
            //提交job
            job.waitForCompletion(true);
        } catch (
                IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
