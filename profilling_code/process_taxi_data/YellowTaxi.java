import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class YellowTaxi {
public static void main(String[] args) throws Exception { if (args.length != 2) {
    System.err.println("Usage: YellowTaxi <input path> <output path>");
      System.exit(-1);
    }
    int i =0;
    int j =0;
    while(i < 6){
     while(j < 6){
    Job job = new Job();
    job.setJarByClass(YellowTaxi.class);
    job.setJobName("RDBA YellowTaxi");
    job.setNumReduceTasks(1);//add to reduce task in 1
    //args[0] = /user/yy1316/Project/cleaned_taxi/taxi_cleaned_
    FileInputFormat.addInputPath(job, new Path(args[0] + "/taxi_cleaned_" + String.valueOf(i+1) + "/part-m-0000" +String.valueOf(j)));
    FileOutputFormat.setOutputPath(job, new Path(args[1]+"/taxi_cleaned_" + String.valueOf(i) + String.valueOf(j)));
    job.setMapperClass(YellowTaxiMapper.class);
    job.setReducerClass(YellowTaxiReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
        if(job.waitForCompletion(true) == true){
            j = j+1;
           }
        }
      j = 0;
      i = i+1;
    }
  }
}
