import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.text.SimpleDateFormat;

public class WeatherMapper extends Mapper< LongWritable, Text, Text, Text > {
    private static final int MISSING= 9999;//
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        
        String line= value.toString();
        //split the value by tab
        String[] inputArr = line.split("\t");
        String inputDate = inputArr[0];
        String rain = inputArr[1];
        String snow = inputArr[2];
        SimpleDateFormat format1=new SimpleDateFormat("MM/dd/yy");
        Date dt1=null;
        try {
            dt1 = format1.parse(inputDate);
        } catch (ParseException e) {
            
            System.err.println("error");
        }
        DateFormat format2=new SimpleDateFormat("EEEE");
        String finalDay=format2.format(dt1);
        if(finalDay!="Saturday"&&finalDay!="Sunday"){
            context.write(new Text(inputDate),new Text(rain));
            context.write(new Text(inputDate),new Text(snow));
        }
        
    }
}


