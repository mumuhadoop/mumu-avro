package com.lovecws.mumu.avro.news;

import com.alibaba.fastjson.JSON;
import com.lovecws.mumu.avro.parser.AvroParser;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.junit.Test;

import java.io.*;
import java.util.Map;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-17 16:39
 */
public class SinaFinanceNewsParser {

    private static AvroParser avroParser = new AvroParser();
    private static final String JAVA_CLASSPATH = SinaFinanceNewsParser.class.getResource("/").getPath();

    @Test
    public void wirte() {
        Schema schema = avroParser.schema("SinaFinanceNewsPair.avsc");

        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter dataFileWriter = new DataFileWriter(writer);
        BufferedReader bufferedReader = null;
        try {
            DataFileWriter fileWriter = dataFileWriter.create(schema, new File(JAVA_CLASSPATH + "financeNews.avro"));
            bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(JAVA_CLASSPATH + "financeNews.json"))));
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
                fileWriter.append(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dataFileWriter.close();
                bufferedReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 读取avro数据
     */
    @Test
    public void read() {
        File file = new File(JAVA_CLASSPATH + "financeNews.avro");
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<GenericRecord>(file, reader);
            Schema schema = dataFileReader.getSchema();
            System.out.println(schema);
            while (dataFileReader.hasNext()) {
                GenericRecord genericRecord = dataFileReader.next();
                System.out.println(genericRecord);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (dataFileReader != null)
                    dataFileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
