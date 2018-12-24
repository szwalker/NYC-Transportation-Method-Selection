import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import java.text.SimpleDateFormat;

public class YellowTaxiMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        //VendorID:int,pickup_datetime:chararray,dropoff_datetime:chararray,trip_distance:float,
        //PULocationID,DOLocationID:int,PULocationID:int
        //Sample input:1       2018-01-01 00:21:05     2018-01-01 00:24:23     0.5     41      24
        String[] Inputlist = line.split("	");
        String pickup_datetime =Inputlist[1];
        SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try{
            Date date = inputFormat.parse(pickup_datetime);
            SimpleDateFormat simpleDateformat = new SimpleDateFormat("EEEE"); // the day of the week abbreviated
            String day_of_week = simpleDateformat.format(date);
            if( !day_of_week.equals("Saturday") && !day_of_week.equals("Sunday")){
                if( date.getHours() > 6 && date.getHours() < 10){ //7-9, 16-18 rush hour
                    String dropoff_datetime = Inputlist[2];
                    Date date2 = inputFormat.parse(dropoff_datetime);
                    long trip_duration = (date2.getTime()-date.getTime())/1000;
                    //key: (pickup_datetime, PULocationID, DOLocationID)
                    //value: VendorID, trip_distance, trip_duration
                    String[] pickup_Date = pickup_datetime.split(" ");
                    String Map_key =  pickup_Date[0] + " " + Inputlist[4] + " " + Inputlist[5];
                    String Map_value = Inputlist[0] + " " + Inputlist[3] + " " + String.valueOf(trip_duration);
                    String rush_hour = "0";
                    context.write(new Text(Map_key), new Text(Map_value + " " + rush_hour));
                }
                if( date.getHours() > 15 && date.getHours() < 19){
                    String dropoff_datetime = Inputlist[2];
                    Date date2 = inputFormat.parse(dropoff_datetime);
                    long trip_duration = (date2.getTime()-date.getTime())/1000;
                    //key: (pickup_datetime, PULocationID, DOLocationID)
                    //value: VendorID, trip_distance, trip_duration
                    String[] pickup_Date = pickup_datetime.split(" ");
                    String Map_key =  pickup_Date[0] + " " + Inputlist[4] + " " + Inputlist[5];
                    String Map_value = Inputlist[0] + " " + Inputlist[3] + " " + String.valueOf(trip_duration);
                    String rush_hour = "1";
                    context.write(new Text(Map_key), new Text(Map_value + " " + rush_hour));

                }
            }
        }
        catch(ParseException ex){
            //Do something
        }

    }
}    

