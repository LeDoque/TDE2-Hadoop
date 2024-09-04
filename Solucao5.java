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

public class Solucao5 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        Path intermediate = new Path("./output/interm.txt");

        // arquivo de saida
        Path output = new Path("output/TDEResp.txt");

        Job j5 = new Job(c, "Vezes por ano");

        j5.setJarByClass(Solucao1.class);
        j5.setMapperClass(MapE.class);
        j5.setReducerClass(ReduceE.class);

        //cadastratr tipos de saida
        j5.setMapOutputKeyClass(Text.class);
        j5.setMapOutputValueClass(LongWritable.class);
        j5.setOutputKeyClass(Text.class);
        j5.setOutputValueClass(LongWritable.class);

        //registros de arquivos de entrada/saida
        FileInputFormat.addInputPath(j5, input);
        FileOutputFormat.setOutputPath(j5, intermediate);

        j5.waitForCompletion(true);
    }

    public static class MapE extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String[] col = linha.split(";");

            String country_or_area = col[0];

            if (country_or_area.equalsIgnoreCase("Brazil")) {
                long price_usd = Long.parseLong(col[5]);
                con.write(new Text("Preco total"), new LongWritable(price_usd));
            }
        }
    }
    public static class ReduceE extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            long soma = 0;
            long count = 0;
            for (LongWritable val : values) {
                soma += val.get();
                count++;
            }

            long media = soma/count;

            con.write(new Text("Media das transações realizadas pelo Brasil:"), new LongWritable(media));
        }
    }
}