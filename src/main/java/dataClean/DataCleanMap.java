package dataClean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 實現自定義Map類，在裡面實現具體的清洗邏輯
 * */
public class DataCleanMap  extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * 1. 從原始數據中過濾出來需要的欄位
     * 2. 針對核心欄位進行異常值判斷
     * */
    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        //獲取每一行數據
        String line = v1.toString();
        //json字符串轉成json對象
        JSONObject jsonObject = JSON.parseObject(line);
        //取得需要的欄位資訊 【註意：在取得數值的時候建議使用getIntValue，如果欄位值缺失，則返回0，這樣就無須在進行判斷】
        if (jsonObject == null) {
            return;
        }
        String uid = jsonObject.getString("uid");
        int gold = jsonObject.getIntValue("gold");
        int watchnumpv = jsonObject.getIntValue("watchnumpv");
        int follower = jsonObject.getIntValue("follower");
        int length = jsonObject.getIntValue("length");

        StringBuilder builder = new StringBuilder();
        //過濾掉異常數據
        if (StringUtils.isNotBlank(uid) && gold >= 0 && watchnumpv >= 0 && follower >= 0 && length >= 0) {
            builder.append(gold).append("\t")
                    .append(watchnumpv).append("\t")
                    .append(follower).append("\t")
                    .append(length);
            context.write(new Text(uid), new Text(builder.toString()));
        }

    }
}
