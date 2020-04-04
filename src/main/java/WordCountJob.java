import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
 * 單詞計數
 *
 * 需求: 讀取hdfs上的hello.txt文件，計算文件中每個單詞出現的總次數
 * hello.txt文件內容如下:
 * hello you
 * hello me
 *
 * 最終需要的結果的形式如下:
 * hello 2
 * me 1
 * you 1
 *
 * */
public class WordCountJob {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        /*
         * 需要實現map函數
         * 這個map函數就是可以接收k1, v1,產生k2, v2
         *
         * */
        @Override
        protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
            // k1代表的就是每一行的行首偏移量，v1代表的是每一行的內容
            // 對取得的每一行數據進行切割，把單詞切割出來
            String[] words = v1.toString().split(" ");
            // 迭代切割出來的單詞數據
            for (String word : words) {
                // 把迭代出來的單詞封裝成<k2,v2>的形式
                Text k2 = new Text(word);
                LongWritable v2 = new LongWritable(1L);
                System.out.println("k2:" + word + "...v2:1");
                // 把<k2,v2>寫出去
                context.write(k2, v2);
            }
        }
    }

    /*
     * 創建自定義的reducer類
     * */
    public static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable> {
        /*
         * 針對v2s的數據進行累加求和，並且最終把數據轉化為k3,v3寫出去
         * */
        @Override
        protected void reduce(Text k2, Iterable<LongWritable> v2s, Context context) throws IOException, InterruptedException {
            // 創建一個sum變量，保存v2s的和
            long sum = 0L;
            for (LongWritable v2 : v2s) {
                sum += v2.get();
            }
            //封裝k3,v3
            Text k3 = k2;
            LongWritable v3 = new LongWritable(sum);
            System.out.println("k3:" + k3.toString() + "......v3:" + sum);
            // 把結果寫出去
            context.write(k3, v3);
        }
    }

    public static void main(String[] args) {
        try {
            if(args.length!=2){
                // 如果傳遞的參數不夠，程式直接跳出
                System.exit(100);
            }
            //Job需要的配置參數
            Configuration conf = new Configuration();
            //創建一個Job
            Job job = Job.getInstance(conf);
            //注意: 這一行必須設定，否則在集群中執行的是找不到WordCountJob這個類
            job.setJarByClass(WordCountJob.class);

            // 指定輸入的路徑(可以是文件，也可以是目錄)
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            //指定輸出路徑(只能指定一個不存在的目錄)
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            //指定map相關的程式碼
            job.setMapperClass(MyMapper.class);
            //指定k2的類型
            job.setMapOutputKeyClass(Text.class);
            //指定v2的類型
            job.setMapOutputValueClass(LongWritable.class);

            //指定reduce相關的程式碼
            job.setReducerClass(MyReduce.class);
            //指定k3的類型
            job.setOutputKeyClass(Text.class);
            //指定v3的類型
            job.setMapOutputValueClass(LongWritable.class);
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
