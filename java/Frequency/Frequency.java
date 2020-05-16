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

public class Frequency
{
    public static class FrequencyMapper 
	extends Mapper<Object, Text, Object, Text>
    {
		private Text word_doc = new Text();
		private Text frequency = new Text();
		private FileSplit split;
		
		@Override
		public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException
        {
			split = (FileSplit)context.getInputSplit();
			StringTokenizer iterator = new StringTokenizer(value.toString()); // ["cafe", "con", "leche", "y", "solo", "cafe"]

			while(iterator.hasMoreTokens())
            {
				word_doc.set( iterator.nextToken() + ":" + split.getPath().toString() ); // "cafe:doc_1.txt"
				frequency.set("1"); // "1"
				
				context.write(word_doc, frequency); // <key, value>  =  <word_doc, frequency>  =  <"cafe:doc_1.txt", "1">
			}
		}
	}

    public static class FrequencyCombiner 
	extends Reducer<Text, Text, Text, Text>
    {
		private Text doc_frequency = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException
        {
			// key = "cafe:doc_1.txt"
			// values = ["1", "1"]

			int frequency = 0;
		
			for (Text value : values) // ["1", "1"]  ->  1 + 1
            {
				frequency += Integer.parseInt(value.toString());
			}

			int separator_index = key.toString().indexOf(":");
			
			doc_frequency.set(key.toString().substring(separator_index + 1) + ":" + frequency); // "doc_1.txt:2"
			key.set(key.toString().substring(0, separator_index)); // "cafe"
			
			context.write(key, doc_frequency); // <key, value>  =  <key, doc_frequency>  =  <"cafe", "doc_1.txt:2">
		}
    }

	public static class FrequencyReducer 
	extends Reducer<Text, Text, Text, Text>
	{
		private Text result = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException
		{
			// key = "cafe"
			// values = ["doc_1.txt:2", "doc_2.txt:1"]

			String docs = new String();

			for (Text value : values) // ["doc_1.txt:2", "doc_2.txt:1"]  ->  "doc_1.txt:2    doc_2.txt:1"
			{
				docs += value.toString() + "\t";
			}

			result.set(docs);
			
			context.write(key, result); // <key, value>  =  <key, result>  =  <"cafe", "doc_1.txt:2    doc_2.txt:1">
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] other_args = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (other_args.length != 2)
		{
			System.err.println("Usage: Frequency <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Frequency");
		
		job.setJarByClass(Frequency.class);
		
		job.setMapperClass(FrequencyMapper.class);
		job.setCombinerClass(FrequencyCombiner.class);
		job.setReducerClass(FrequencyReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(other_args[0]));
		FileOutputFormat.setOutputPath(job, new Path(other_args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
