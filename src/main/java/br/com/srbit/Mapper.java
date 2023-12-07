package br.com.srbit;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.StringTokenizer;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable numeroUm = new IntWritable(1);
    private final Text palavra = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tk = new StringTokenizer(value.toString());

        while (tk.hasMoreTokens()) {
            String token = tk.nextToken();
            if (token.equals("\"NumberOfPrompts\":")) {
                String data = tk.nextToken();
                System.out.println(data);
                palavra.set(data);
                context.write(palavra, numeroUm);
            }
            if (token.equals("\"Type\":")) {
                String tipo = tk.nextToken();
                System.out.println(tipo);
                palavra.set(tipo);
                context.write(palavra, numeroUm);
            }
        }
    }
}
