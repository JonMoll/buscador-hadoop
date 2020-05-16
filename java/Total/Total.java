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

// Para los documentos:
// doc_1.txt: cafe con leche y solo cafe
// doc_2.txt: cafe negro con leche

public class Total
{
    public static class TotalMapper 
	extends Mapper<Object, Text, Object, Text>
    {
		private Text doc = new Text();
		private Text counter = new Text();
		private FileSplit split;
		
		@Override
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
        {
			split = (FileSplit)context.getInputSplit();
			StringTokenizer iterator = new StringTokenizer(value.toString()); // ["cafe", "con", "leche", "y", "solo", "cafe"]

			while(iterator.hasMoreTokens())
            {
				iterator.nextToken();
                doc.set(split.getPath().toString()); // "doc_1.txt"
				counter.set("1"); // "1"
				
				context.write(doc, counter); // <key, value>  =  <doc, counter>  =  <"doc_1.txt", "1">
			}
		}
	}

    public static class TotalReducer
	extends Reducer<Text, Text, Text, Text>
    {
		private Text total = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException
        {
            // key = "doc_1.txt"
		    // values = ["1", "1", "1", "1", "1", "1"]

			int sum = 0;
		
			for (Text value : values) // ["1", "1", "1", "1", "1", "1"]  ->  1 + 1 + 1 + 1 + 1 + 1
            {
				sum += Integer.parseInt(value.toString());
			}
			
			total.set(Integer.toString(sum)); // "6"
			
			context.write(key, total); // <key, value>  =  <key, total>  =  <"doc_1.txt", "6">
		}
    }

    public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] other_args = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (other_args.length != 2)
		{
			System.err.println("Usage: Total <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Total");
		
		job.setJarByClass(Total.class);
		
        job.setMapperClass(TotalMapper.class);
		job.setReducerClass(TotalReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(other_args[0]));
		FileOutputFormat.setOutputPath(job, new Path(other_args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
