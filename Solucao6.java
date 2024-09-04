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

public class Solucao6 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        Path intermediate = new Path("./output/interm.txt");

        // arquivo de saida
        Path output = new Path("output/TDEResp.txt");

        Job j6 = new Job(c, "Vezes por ano");

        j6.setJarByClass(Solucao1.class);
        j6.setMapperClass(MapF.class);
        j6.setReducerClass(ReduceF.class);

        //cadastratr tipos de saida
        j6.setMapOutputKeyClass(Text.class);
        j6.setMapOutputValueClass(LongWritable.class);
        j6.setOutputKeyClass(Text.class);
        j6.setOutputValueClass(LongWritable.class);

        //registros de arquivos de entrada/saida
        FileInputFormat.addInputPath(j6, input);
        FileOutputFormat.setOutputPath(j6, intermediate);

        j6.waitForCompletion(true);
    }

    public static class MapF extends Mapper<LongWritable, Text, Text, LongWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            String[] col = linha.split(";");

            String country_or_area = col[0];

            String year = col[1];

            if (country_or_area.equalsIgnoreCase("Brazil") && year.equalsIgnoreCase("2016")) {
                long price_usd = Long.parseLong(col[5]);
                con.write(new Text("Preco total"), new LongWritable(price_usd));
            }
        }
    }
    public static class ReduceF extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context con)
                throws IOException, InterruptedException {

            long valor_Max = 0;
            long valor_Min = 1000;

            for (LongWritable val : values) {
                long price = val.get();
                if (price > valor_Max) {
                    valor_Max = price;
                }
                if (price < valor_Min && price != 0) {
                    valor_Min = price;
                }
            }
            con.write(new Text("Transação mais cara: "), new LongWritable(valor_Max));

            con.write(new Text("Transação mais barata: "), new LongWritable(valor_Min));
        }
    }
}