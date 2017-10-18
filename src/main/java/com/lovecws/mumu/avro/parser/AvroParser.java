package com.lovecws.mumu.avro.parser;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 解析avsc
 * @date 2017-10-17 11:44
 */
public class AvroParser {

    /**
     * 获取schema
     *
     * @return
     */
    public Schema schema(String schemaFile) {
        Schema.Parser parser = new Schema.Parser();
        InputStream resourceAsStream = AvroParser.class.getClassLoader().getResourceAsStream(schemaFile);
        Schema schema = null;
        try {
            schema = parser.parse(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                resourceAsStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return schema;
    }

    /**
     * 将对象序列化成字节数组
     *
     * @param schemaFile
     * @return
     */
    public byte[] serialize(String schemaFile) {
        Schema schema = schema(schemaFile);
        GenericRecord record = new GenericData.Record(schema);
        record.put("left", "lover");
        record.put("right", "cws");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        try {
            writer.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 反序列化 将字节数组反序列化成对象
     *
     * @param schemaFile
     * @param bs
     */
    public void deserialize(String schemaFile, byte[] bs) {
        Schema schema = schema(schemaFile);
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bs, null);
        try {
            GenericRecord record = reader.read(null, decoder);
            System.out.println(record);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
