package dataClean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * 數據清洗作業
 * 需求:
 * 1. 從原始數據(JSON格式)中過濾出來需要的欄位
 *  主播ID(uid)、金幣數量(gold)、總觀看PV(watchnumpv)、粉絲關注數量(follower)、視頻總開播時長(length)
 * 2. 針對核心欄位進行異常值判斷
 *  金幣數量、總觀看PV、粉絲關注數量、視頻總開播時長
 *  以上四個欄位正常情況都不應該為負值，也不應該缺失
 *  如果這些欄位直為負值，則認為是異常數據，直接丟棄，如果這些欄位植個別有缺失，則認為欄位的值為0
 *
 * 分析:
 * 1. 由於原始數據是json格式，所以可以使用fastjson對原始數據進行解析，取得指定欄位的內容
 * 2. 然後對取得的數據進行判斷，只保留滿足條件的數據即可
 * 3. 由於不需要聚合過程，只是一個簡單的過濾操作，所以只需要map階段即可，reduce階段不需要
 * 4. 其中map階段的k1,v1的數據類型是固定的: <LongWritable, Text>
 *      k2, v2的數據類型為: <Text,Text> k2存儲主播ID, v2存儲核心欄位，多個欄位中間用\t分割
 * */
public class DataCleanJob {
    public static void main(String[] args) throws Exception {
        /**
        if (args.length != 2) {
            System.exit(100);
        }
         */
        // Job需要配置的參數
        Configuration configuration = new Configuration();
        // 創建一個Job
        Job job = Job.getInstance(configuration);
        job.setJarByClass(DataCleanJob.class);

        // 指定輸入的路徑(可以是文件，也可以是目錄)
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        //指定輸出路徑(只能指定一個不存在的目錄)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //指定Map相關程式碼
        job.setMapperClass(DataCleanMapper.class);
        //指定k2的類型
        job.setMapOutputKeyClass(Text.class);
        //指定v2的類型
        job.setMapOutputValueClass(Text.class);

        //禁用reduce
        job.setNumReduceTasks(0);

//        FileInputFormat.setInputPaths(job, new Path("video/input/video.log"));
//        Path outputPath = new Path("video/etl");
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        Path outputPath = new Path(args[1]);
//        FileSystem fs = FileSystem.get(configuration);
//        if (fs.exists(outputPath)) {
//            fs.delete(outputPath, true);
//        }
//        FileOutputFormat.setOutputPath(job, outputPath);

        boolean result = job.waitForCompletion(true);
        System.out.println("result: " + result);
    }
}
