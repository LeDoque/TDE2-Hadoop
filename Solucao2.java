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

public class Solucao2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        Path intermediate = new Path("./output/interm.txt");

        // arquivo de saida
        Path output = new Path("output/TDEResp.txt");

        Job j2 = new Job(c, "Vezes por ano");

        j2.setJarByClass(Solucao1.class);
        j2.setMapperClass(MapB.class);
        j2.setReducerClass(ReduceB.class);

        //cadastratr tipos de saida
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(LongWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(LongWritable.class);

        //registros de arquivos de entrada/saida
        FileInputFormat.addInputPath(j2, input);
        FileOutputFormat.setOutputPath(j2, intermediate);

        j2.waitForCompletion(true);
    }

    public static class MapB extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String[] col = linha.split(";");

            String year = col[1];

            con.write(new Text(year), new LongWritable(1));
        }
    }
    public static class ReduceB extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {
            long soma = 0;
            for (LongWritable vzs : values) {
                soma += vzs.get();
            }
            con.write(key, new LongWritable(soma));
        }
    }
}
