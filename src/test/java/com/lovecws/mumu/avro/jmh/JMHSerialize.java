package com.lovecws.mumu.avro.jmh;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.avro.news.SinaFinanceNewsParser;
import com.lovecws.mumu.avro.parser.AvroParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-18 9:41
 */
public class JMHSerialize {

    private static Schema schema = new AvroParser().schema("SinaFinanceNewsPair.avsc");
    private static final String JAVA_CLASSPATH = SinaFinanceNewsParser.class.getResource("/").getPath();
    private static final DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);

    private static List<GenericRecord> records = new ArrayList<GenericRecord>();

    static {
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(JAVA_CLASSPATH + "financeNews.json"))));
            String readLine = null;
            while ((readLine = bufferedReader.readLine()) != null) {
                if (readLine == null) {
                    continue;
                }
                Map map = JSON.parseObject(readLine, Map.class);
                GenericRecord record = new GenericData.Record(schema);
                record.put("htitle", map.get("htitle"));
                record.put("keywords", map.get("keywords"));
                record.put("description", map.get("description"));
                record.put("url", map.get("url"));
                record.put("sumary", map.get("sumary"));
                record.put("content", map.get("content"));
                record.put("logo", map.get("logo"));
                record.put("title", map.get("title"));
                record.put("pubDate", map.get("pubDate"));
                record.put("mediaName", map.get("mediaName"));
                record.put("mediaUrl", map.get("mediaUrl"));
                record.put("category", map.get("category"));
                record.put("type", map.get("type"));
                records.add(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.All)
    public void serialize() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        for (GenericRecord record : records) {
            writer.write(record, encoder);
        }
        //byte[] bytes = byteArrayOutputStream.toByteArray();
        encoder.flush();
        //System.out.println(bytes.length);
    }

    public static void main(String[] args) {
        Options options = new OptionsBuilder()
                .include(JMHSerialize.class.getSimpleName()+".serialize")
                .threads(1)
                .forks(1)
                .measurementIterations(10)
                .warmupIterations(10)
                .build();
        try {
            new Runner(options).run();
        } catch (RunnerException e) {
            e.printStackTrace();
        }
    }
}
