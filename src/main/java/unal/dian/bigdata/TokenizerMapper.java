package unal.dian.bigdata;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, FloatWritable>{

    private Text date = new Text();
    private Text hour = new Text();
    private Text store = new Text();
    private Text product = new Text();
    private FloatWritable price = new FloatWritable();
    private Text payment = new Text();


    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        StringTokenizer tokens = new StringTokenizer(value.toString(), "\t");
        while (tokens.hasMoreTokens()) {
            date.set(tokens.nextToken());
            hour.set(tokens.nextToken());
            store.set(tokens.nextToken());
            product.set(tokens.nextToken());
            price.set(Float.parseFloat(tokens.nextToken()));
            payment.set(tokens.nextToken());
            context.write(payment, price);
        }
    }
}
