package com.lovecws.mumu.avro.parser;

import avro.StringPair;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-17 14:06
 */
public class SpecialAvroParser {

    public static void main(String[] args) {
        StringPair stringPair = new StringPair();
        stringPair.setLeft("baby");
        stringPair.setRight("mumu");
        System.out.println(stringPair);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DatumWriter<StringPair> writer = new SpecificDatumWriter<StringPair>(StringPair.class);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        try {
            writer.write(stringPair, encoder);
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
    }
}
