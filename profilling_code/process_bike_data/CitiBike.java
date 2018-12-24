import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CitiBike {
    
    
    
    public static void main(String[] args) throws Exception{
        // TODO Auto-generated method stub
        if(args.length!= 2){
            System.err.println("Usage: Citibike <input path>;<output path>");
            System.exit(-1);
        }
        Job job= new Job();
        job.setJarByClass(CitiBike.class);
        job.setJobName("CitiBike");
        
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        
        job.setMapperClass(CitibikeMapper.class);
        job.setReducerClass(CitibikeReducer.class);
        
        job.setNumReduceTasks(1);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
    
}
