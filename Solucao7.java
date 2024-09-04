package TDE2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Solucao7 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        Path intermediate = new Path("./output/interm.txt");

        // arquivo de saida
        Path output = new Path("output/TDEResp.txt");

        Job j7 = new Job(c, "Vezes por ano");

        j7.setJarByClass(Solucao1.class);
        j7.setMapperClass(MapG.class);
        j7.setReducerClass(ReduceG.class);

        //cadastratr tipos de saida
        j7.setMapOutputKeyClass(Text.class);
        j7.setMapOutputValueClass(LongWritable.class);
        j7.setOutputKeyClass(Text.class);
        j7.setOutputValueClass(LongWritable.class);

        //registros de arquivos de entrada/saida
        FileInputFormat.addInputPath(j7, input);
        FileOutputFormat.setOutputPath(j7, intermediate);

        j7.waitForCompletion(true);
    }

    public static class MapG extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String[] col = linha.split(";");

            String country_or_area = col[0];

            String flow = col[4];

            if (country_or_area.equalsIgnoreCase("Brazil") && flow.equalsIgnoreCase("Export")) {
                long year = Long.parseLong(col[1]);
                long trade_usd = Long.parseLong(col[5]);
                con.write(new Text(String.valueOf(year)), new LongWritable(trade_usd));
            }
        }
    }
    public static class ReduceG extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            long soma = 0;
            long count = 0;
            for (LongWritable val : values) {
                soma += val.get();
                count++;
            }

            long media = soma/count;
            con.write(key, new LongWritable(media));
        }
    }
}