import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class LastTimeCount {
    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);
        Job j = new Job(c, "lasttimecount");
        j.setJarByClass(TypeCount.class);
        j.setMapperClass(MapForLastTimeCount.class);
        j.setReducerClass(ReduceForLastTimeCount.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForLastTimeCount extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context con) {
            try {
                String line = value.toString();
                String User = line.split("\t")[1];
                String startTime = line.split("\t")[9];
//              String endTime = line.split("\t")[10];
                String rawDur = line.split("\t")[11];
                /**
                 *
                 * Key     Text : 电话号码 时间段id
                 * Value        : 时间长度
                 * 时间段id：
                 * 1： 0-3
                 * 2： 3-6
                 * 3： 6-9
                 * 4： 9-12
                 * 5： 12-15
                 * 6： 15-18
                 * 7： 18-21
                 * 8： 21-24
                 *
                 *
                 *
                 *
                 *
                 * */
                for (int i = 1; i <= 8; i++) {
                    con.write(new Text(User + "   时间段id：" + String.valueOf(i)), new IntWritable(LastTimeCount.isInPeriod(startTime, i, Integer.parseInt(rawDur))));
                }

            } catch (Exception e) {
                System.out.println(e);
            }
        }
    }

    public static class ReduceForLastTimeCount extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text word, Iterable<IntWritable> values, Context con) {
            try {
                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                con.write(word, new IntWritable(sum));
            } catch (Exception e) {
                System.out.println(e);
            }

        }

    }

    public static int isInPeriod(String start, int id, int lastSec) {
        //id 从1开始，表示时间段的编号

        int hour_start = Integer.parseInt(start.split(":")[0]);
        int min_start = Integer.parseInt(start.split(":")[1]);
        int sec_start = Integer.parseInt(start.split(":")[2]);
        int sec_end = sec_start + lastSec % 60;
        boolean secflag = false;
        if (sec_end >= 60) {
            sec_end -= 60;
            secflag=true;
        }
        int min_end = min_start + lastSec % 3600 / 60;
        boolean minflag= false;
        if(secflag){
            min_end += 1;
            if(min_end>=60){
                min_end -=60;
                minflag =true;
            }
        }
        int hour_end = hour_start + lastSec / 3600;
        if(minflag){
            hour_end+=1;
        }
        System.out.println(hour_start+" "+ hour_end+ " "+min_start+" "+min_end+" "+sec_start+" "+sec_end);
        int time_slot_start = (id - 1) * 3;
        int time_slot_end = time_slot_start + 3;
        //在中间
        if (hour_start >= time_slot_start && hour_end < time_slot_end) {
            System.out.println("1:" + String.valueOf((hour_end - hour_start) * 3600 + (min_end - min_start) * 60 + sec_end - sec_start));
            return (hour_end - hour_start) * 3600 + (min_end - min_start) * 60 + sec_end - sec_start;
        }
        //包含该时段
        else if (hour_start < time_slot_start && hour_end >= time_slot_end) {
            return 3 * 3600;
        }
        //跨前一段和这段
        else if (hour_end >= time_slot_start && hour_start < time_slot_start) {
            System.out.println("2:" + String.valueOf((hour_end - time_slot_start) * 3600 + min_end * 60 + sec_end));
            return (hour_end - time_slot_start) * 3600 + min_end * 60 + sec_end;
        }
        //跨当前段和后一段
        else if (hour_start < time_slot_end && hour_end >= time_slot_end) {
            System.out.println("3:" + String.valueOf((time_slot_end - hour_start) * 3600 + (60 - min_end) * 60 + 60 - sec_end));
            return (time_slot_end - hour_start) * 3600 + (60 - min_end) * 60 + 60 - sec_end;
        } else {
            return 0;
        }
    }

}