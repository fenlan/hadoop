import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMapReduce {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");

        job.setJarByClass(WordCountMapReduce.class);

        job.setMapperClass(WordCountMapper.class);

        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // FileInputFormat.setInputPaths(job, new Path("hdfs://hadoop-master:9000/fenlan/input/hello"));
        FileInputFormat.setInputPaths(job, new Path("/home/fenlan/IdeaProjects/hadoop/input/hello"));
        // FileOutputFormat.setOutputPath(job, new Path("hdfs://hadoop-master:9000/fenlan/output/word"));
        FileOutputFormat.setOutputPath(job, new Path("/home/fenlan/IdeaProjects/hadoop/output/words"));

        boolean b = job.waitForCompletion(true);
        if (!b) {
            System.err.println("This task has failed!");
        }
    }
}
