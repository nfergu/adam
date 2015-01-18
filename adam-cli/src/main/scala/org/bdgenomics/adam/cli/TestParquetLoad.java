package org.bdgenomics.adam.cli;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bdgenomics.formats.avro.Genotype;
import org.bdgenomics.formats.avro.Variant;
import parquet.Log;
import parquet.avro.AvroReadSupport;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class TestParquetLoad extends Configured implements Tool {

    private static final Log LOG = Log.getLog(TestParquetLoad.class);

    private static long total = 0;

    private static final Set<VariantWrapper> variants = new HashSet<VariantWrapper>();

    private static long startTime;

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new TestParquetLoad(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public int run(String[] args) throws Exception {

        if (args.length < 1) {
            LOG.error("Usage: " + getClass().getName() + " <input>");
            return 1;
        }

        String inputDirectory = args[0];

        Configuration configuration = getConf();

        Job job = new Job(configuration);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        job.setMapperClass(ReadRequestMap.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(ParquetInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        ParquetInputFormat.setReadSupportClass(job, AvroReadSupport.class);

        FileInputFormat.setInputPaths(job, new Path(inputDirectory));

        startTime = System.nanoTime();

        job.waitForCompletion(true);

        long endTime = System.nanoTime();

        LOG.info("Read " + total + " records in " + ((endTime - startTime) / 1000000) + " ms");

        return 0;

    }

    public static class ReadRequestMap extends Mapper<Object, Object, Void, Void> {
        @Override
        public void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            if (value instanceof  Variant) {
                org.bdgenomics.formats.avro.Variant variant = (Variant) value;
                variants.add(new VariantWrapper(variant));
                total++;
                System.out.println(variant);
                if (total % 100000 == 0) {
                    long endTime = System.nanoTime();
                    long timeNanos = (endTime - startTime);
                    System.out.println("Read " + total + " records in " + (timeNanos / 1000000) + " ms");
                    System.out.println("That's " + (total / (timeNanos / 1000000000)) + " records / sec");
                    System.out.println("Read " + variants.size() + " unique records");
                }
            }
            else {
                System.out.println("Bad class [" + value.getClass() + "]");
            }
        }
    }

    private static final class VariantWrapper {
        private Variant variant;
        public VariantWrapper(Variant variant) {
            this.variant = variant;
        }
        @Override
        public boolean equals(Object o) {
            VariantWrapper that = (VariantWrapper) o;
            return this.variant.getStart().equals(that.variant.getStart()) &&
                    this.variant.getEnd().equals(that.variant.getEnd()) &&
                    this.variant.getReferenceAllele().equals(that.variant.getReferenceAllele()) &&
                    this.variant.getAlternateAllele().equals(that.variant.getAlternateAllele()) &&
                    this.variant.getContig().getContigName().equals(that.variant.getContig().getContigName());
        }
        @Override
        public int hashCode() {
            int result = this.variant.getStart().hashCode();
            result = 31 * result + this.variant.getEnd().hashCode();
            result = 31 * result + this.variant.getReferenceAllele().hashCode();
            result = 31 * result + this.variant.getAlternateAllele().hashCode();
            result = 31 * result + this.variant.getContig().getContigName().hashCode();
            return result;
        }
    }

}
