import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TypeCount {
    public static void main(String [] args) throws Exception
    {
        Configuration c=new Configuration();
        String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
        Path input=new Path(files[0]);
        Path output=new Path(files[1]);
        Job j=new Job(c,"typecount");
        j.setJarByClass(TypeCount.class);
        j.setMapperClass(MapForTypeCount.class);
        j.setReducerClass(ReduceForTypeCount.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);
        System.exit(j.waitForCompletion(true)?0:1);
    }
    public static class MapForTypeCount extends Mapper<LongWritable, Text, Text, IntWritable>{
        public void map(LongWritable key, Text value, Context con)
        {
            try{
                String line = value.toString();
                String callType = line.split("\t")[12];
                String call = line.split("\t")[3];
                String called = line.split("\t")[4];
                /**
                 * call & called: 1：电信 2：移动 3：联通 4：不详
                 * callType: 1：市话 2：长途 3：漫游
                 *
                 * 5  电信 市话
                 * 6  移动 市话
                 * 7  联通 市话
                 * 8  不详 市话
                 * 9  电信 长途
                 * 10 移动 长途
                 * 11 联通 长途
                 * 12 不详 长途
                 * 13 电信 漫游
                 * 14 移动 漫游
                 * 15 联通 漫游
                 * 16 不详 漫游
                 *
                 */
                IntWritable outputValue = new IntWritable(1);
                con.write(new Text(String.valueOf(Integer.parseInt(call)+Integer.parseInt(callType)*4)),outputValue);
                con.write(new Text(String.valueOf(Integer.parseInt(called)+Integer.parseInt(callType)*4)),outputValue);
            }catch (Exception e){
                System.out.println(e);
            }
        }
    }
    public static class ReduceForTypeCount extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text word, Iterable<IntWritable> values, Context con)
        {
            try{
                int sum = 0;
                for(IntWritable value : values)
                {
                    sum += value.get();
                }
                con.write(word, new IntWritable(sum));
            }catch (Exception e){
                System.out.println(e);
            }

        }

    }

}