package com.lovecws.mumu.avro.parser;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: TODO
 * @date 2017-10-17 13:42
 */
public class AvroParserTest {

    @Test
    public void avro() {
        AvroParser avroParser = new AvroParser();
        String schemaFile = "StringPair.avsc";
        byte[] bytes = avroParser.serialize(schemaFile);
        avroParser.deserialize(schemaFile, bytes);
    }
}
