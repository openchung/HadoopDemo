package videoInfo;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定義數據類型
 * 主要是為了保存主播相關的核心欄位，方便後期進行累加操作
 * */
public class VideoInfoWritable implements Writable {

    private long gold;
    private long watchnumpv;
    private long follower;
    private long length;

    public VideoInfoWritable() {}

    public VideoInfoWritable(long gold, long watchnumpv, long follower, long length) {
        this.gold = gold;
        this.watchnumpv = watchnumpv;
        this.follower = follower;
        this.length = length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(gold);
        out.writeLong(watchnumpv);
        out.writeLong(follower);
        out.writeLong(length);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.gold = dataInput.readLong();
        this.watchnumpv = dataInput.readLong();
        this.follower = dataInput.readLong();
        this.length = dataInput.readLong();
    }

    @Override
    public String toString() {
        return gold + "\t" + watchnumpv + "\t" + follower + "\t" + length;
    }

    public long getGold() {
        return gold;
    }

    public void setGold(long gold) {
        this.gold = gold;
    }

    public long getWatchnumpv() {
        return watchnumpv;
    }

    public void setWatchnumpv(long watchnumpv) {
        this.watchnumpv = watchnumpv;
    }

    public long getFollower() {
        return follower;
    }

    public void setFollower(long follower) {
        this.follower = follower;
    }

    public long getLength() {
        return length;
    }

    public void setLength(long length) {
        this.length = length;
    }
}
