package videoInfo;

import dataClean.DataCleanJob;
import dataClean.DataCleanMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 需求:
 * 1. 基於主播進行統計，統計每個主播在當天收到的總金幣數量，總觀看PV，總關注數量，總視頻開播時長
 * 分析:
 * 1. 為了方便統計主播的指標數據，最好視把這些欄位整合到一個對象中，這樣維護起來比較方便，這樣就需要自定義writable
 * 2. 由於在這裡需要以主播維度進行數據的整合，所以需要以主播ID作為key進行聚合統計
 * 3. 所以Map節點的<k2,v2>為<Text,自定義writable>
 * 4. 由於需要聚合，所以Reduce階段也需要有
 *
 * @Classname: VideoInfoTopJob
 * @Author: Ming
 * @Date: 2020/1/27 7:49 下午
 * @Version: 1.0
 * @Description: 統計每天開播時長最長的前10名主播及對應的開播時長
 **/
public class VideoInfoJob {
    public static void main(String[] args) throws Exception {
        try {
            /**
             if (args.length != 2) {
             System.exit(100);
             }
             */
            // Job需要配置的參數
            Configuration configuration = new Configuration();
            // 創建一個Job
            Job job = Job.getInstance(configuration);
            FileSystem fs = FileSystem.get(configuration);
//        Path outputPath = new Path("video/info");
            Path outputPath = new Path(args[1]);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }
            job.setJarByClass(VideoInfoJob.class);

            // 指定輸入的路徑(可以是文件，也可以是目錄)
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            //指定輸出路徑(只能指定一個不存在的目錄)
            FileOutputFormat.setOutputPath(job, new Path(args[1]));

            //指定Map相關程式碼
            job.setMapperClass(VideoInfoMapper.class);
            //指定k2的類型
            job.setMapOutputKeyClass(Text.class);
            //指定v2的類型
            job.setMapOutputValueClass(VideoInfoWritable.class);

            //指定reduce相關的程式碼
            job.setReducerClass(VideoInfoReducer.class);
            //指定k3的類型
            job.setOutputKeyClass(Text.class);
            //指定v3的類型
            job.setMapOutputValueClass(VideoInfoWritable.class);
            //提交job
            job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
