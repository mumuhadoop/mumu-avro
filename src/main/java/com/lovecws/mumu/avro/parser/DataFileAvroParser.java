package com.lovecws.mumu.avro.parser;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-17 14:22
 */
public class DataFileAvroParser {

    private static AvroParser avroParser = new AvroParser();
    private static final String JAVA_CLASSPATH = DataFileAvroParser.class.getResource("/").getPath();

    /**
     * 写入avro数据
     */
    public void write() {
        Schema schema = avroParser.schema("StringPair.avsc");
        GenericRecord record = new GenericData.Record(schema);
        record.put("left", "lover");
        record.put("right", "cws");

        File file = new File(JAVA_CLASSPATH + "data.avro");
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter dataFileWriter = new DataFileWriter(writer);
        try {
            DataFileWriter fileWriter = dataFileWriter.create(schema, file);
            for (int i = 0; i < 1000; i++) {
                fileWriter.append(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dataFileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 读取avro数据
     */
    public void read() {
        File file = new File(JAVA_CLASSPATH + "data.avro");
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
