package utils;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Classname: DateUtils
 * @Author: Danniel
 * @Date: 2020/4/5 8:58 下午
 * @Version: 1.0
 * @Description: 日期工具類
 **/
public class DateUtils {

    private static SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");

    /**
     * 轉換日期格式
     * 從yyyyMMdd轉換成yyyy-MM-dd
     * @param dateTime
     * @return
     */
    public static String transDateFormat(String dateTime) {
        String res = "1970-01-01";
        try {
            Date date = sdf1.parse(dateTime);
            res = sdf2.format(date);
        } catch (Exception e) {
            System.out.println("日期轉換失敗：" + dateTime);
        }
        return res;
    }

}
