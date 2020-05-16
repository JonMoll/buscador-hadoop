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

public class InvertedIndex 
{
	public static class InvertedIndexMapper 
	extends Mapper<Object,Text,Object,Text>
    {
		private Text word = new Text();
        private Text doc = new Text();
		private FileSplit split;
		
		@Override
		public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException
        {
			split = (FileSplit)context.getInputSplit();
			StringTokenizer iterator = new StringTokenizer(value.toString()); // ["cafe", "con", "leche", "y", "solo", "cafe"]

			while(iterator.hasMoreTokens())
            {
				word.set(iterator.nextToken()); // "cafe"
                doc.set(split.getPath().toString()); // "doc_1.txt"
				
				context.write(word, doc); // <key, value>  =  <word, doc>  =  <"cafe", "doc_1.txt">
			}
		}
	}

	public static class InvertedIndexReducer
	extends Reducer<Text, Text, Text, Text>
    {
		private Text result = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException
        {
            // key = "cafe"
		    // values = ["doc_1.txt", "doc_1.txt", "doc_2.txt"]

			String docs = new String();
		
			for (Text value : values) // ["doc_1.txt", "doc_2.txt"]  ->  "doc_1.txt    doc_2.txt"
            {
				docs += value.toString() + "\t";
			}

			result.set(docs);
			
			context.write(key, result); // <key, value>  =  <key, result>  =  <"cafe", "doc_1.txt   doc_1.txt    doc_2.txt">
		}	
	}

	public static void main(String[] args) throws Exception
    {
		Configuration conf = new Configuration();
		String[] other_args = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (other_args.length != 2)
        {
			System.err.println("Usage: InvertedIndex <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "InvertedIndex");
		
		job.setJarByClass(InvertedIndex.class);
		
        job.setMapperClass(InvertedIndexMapper.class);
		job.setReducerClass(InvertedIndexReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(other_args[0]));
		FileOutputFormat.setOutputPath(job, new Path(other_args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
