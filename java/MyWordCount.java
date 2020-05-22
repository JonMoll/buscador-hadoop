import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// Para el documento:
// doc_1.txt: cafe con leche y solo cafe

public class MyWordCount
{
    public static class MyWordCountMapper
	extends Mapper<Object, Text, Object, Text>
    {
		private Text word = new Text();
		private Text frequency = new Text();
		
		@Override
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
        {
			StringTokenizer iterator = new StringTokenizer(value.toString()); // ["cafe", "con", "leche", "y", "solo", "cafe"]

			while(iterator.hasMoreTokens())
            {
				word.set(iterator.nextToken()); // "cafe"
				frequency.set("1"); // "1"
				
				context.write(word, frequency); // <key, value>  =  <word, frequency>  =  <"cafe", "1">
			}
		}
	}

    public static class MyWordCountReducer
	extends Reducer<Text, Text, Text, Text>
    {
		private Text total_frequency = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException
        {
			// key = "cafe"
			// values = ["1", "1"]

			int accumulated = 0;
		
			for (Text value : values) // ["1", "1"]  ->  1 + 1
            {
				accumulated += Integer.parseInt(value.toString());
			}
			
			total_frequency.set(Integer.toString(accumulated)); // "2"
			context.write(key, total_frequency); // <key, value>  =  <key, total_frequency>  =  <"cafe", "2">
		}
    }

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] other_args = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = new Job(conf, "MyWordCount");
		
		job.setJarByClass(MyWordCount.class);
		
		job.setMapperClass(MyWordCountMapper.class);
		job.setReducerClass(MyWordCountReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(other_args[0]));
		FileOutputFormat.setOutputPath(job, new Path(other_args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
