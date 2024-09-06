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

public class Solucao8 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        Path intermediate = new Path("./output/interm.txt");

        // arquivo de saida
        Path output = new Path("output/TDEResp.txt");

        Job j8 = new Job(c, "Vezes por ano");

        j8.setJarByClass(Solucao8.class);
        j8.setMapperClass(MapH.class);
        j8.setReducerClass(ReduceH.class);

        //cadastratr tipos de saida
        j8.setMapOutputKeyClass(WritableAnoPais.class);
        j8.setMapOutputValueClass(DoubleWritable.class);
        j8.setOutputKeyClass(WritableAnoPais.class);
        j8.setOutputValueClass(Text.class);

        //registros de arquivos de entrada/saida
        FileInputFormat.addInputPath(j8, input);
        FileOutputFormat.setOutputPath(j8, intermediate);

        j8.waitForCompletion(true);
    }

    public static class MapH extends Mapper<LongWritable, Text, WritableAnoPais, DoubleWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String[] col = linha.split(";");

            if (col[0].equals("country_or_area")) return;

            if (!col[8].equals("No Quantity") && !col[8].isEmpty()) {
                long trade_usd = Long.parseLong(col[5]);
                double qtd = Double.parseDouble(col[8]);

                if (qtd > 0) {
                    double precoQtd = trade_usd / qtd;
                    con.write(new WritableAnoPais(col[1], col[0]), new DoubleWritable(precoQtd));
                }
            }
        }
    }

    public static class ReduceH extends Reducer<WritableAnoPais, DoubleWritable, WritableAnoPais, Text> {
        public void reduce(WritableAnoPais key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double valor_Max = 0;
            double valor_Min = 1000;

            for (DoubleWritable val : values) {
                double price = val.get();

                if (price > valor_Max) {
                    valor_Max = price;
                }
                if (price < valor_Min) {
                    valor_Min = price;
                }
            }
            con.write(key, new Text("Transação mais cara: " + valor_Max + ", Transação mais barata: " + valor_Min));
        }
    }

}

