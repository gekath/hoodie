package com.uber.hoodie.common;

import com.uber.hoodie.avro.MercifulJsonConverter;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Optional;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Created by kge on 2017-05-18.
 */
public class HoodieRowPayload implements HoodieRecordPayload<HoodieRowPayload> {
    private String partitionPath;
    private String rowKey;
    private byte[] jsonDataCompressed;
    private int dataSize;
    private static Logger logger = LogManager.getLogger(HoodieRowPayload.class);

    public HoodieRowPayload(Optional<String> jsonData, String rowKey, String partitionPath) throws IOException {
        if(jsonData.isPresent()) {
            this.jsonDataCompressed = compressData(jsonData.get());
            this.dataSize = jsonData.get().length();
        }
        this.rowKey = rowKey;
        this.partitionPath = partitionPath;
    }

    @Override
    public HoodieRowPayload preCombine(HoodieRowPayload another) {
        return this;
    }

    @Override
    public java.util.Optional<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema)
            throws IOException {
        return getInsertValue(schema);
    }

    @Override
    public java.util.Optional<IndexedRecord> getInsertValue(Schema schema) throws IOException {

        MercifulJsonConverter jsonConverter = new MercifulJsonConverter(schema);
        GenericRecord record = jsonConverter.convert(getJsonData());

        try {
            record.put(HoodieRecord.DELETE_FIELD, false);
            record.put(HoodieRecord.DELETED_AT_FIELD, null);
        } catch (IllegalArgumentException e) {
        } catch (java.lang.RuntimeException e2) {
        }

        return java.util.Optional.of(record);
    }
//
//        Iterator<Schema.Field> iter = record.getSchema().getFields().iterator();
//        while (iter.hasNext()) {
//            Schema.Field field = iter.next();
//            try {
//
////                ((GenericRecord) record).put(field.name(), row.getAs(field.name()));
//
//                // This should be where the insert value modifies field to false again
//                ((GenericRecord) record).put(HoodieRecord.DELETE_FIELD, false);
//                ((GenericRecord) record).put(HoodieRecord.DELETED_AT_FIELD, null);
//            } catch (IllegalArgumentException e) {
//            } catch (java.lang.RuntimeException e2) {
//            }
//        }
//        return java.util.Optional.of(record);
//    }

    private String getJsonData() throws IOException {
        return unCompressData(jsonDataCompressed);
    }


    private byte[] compressData(String jsonData) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream dos =
                new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_COMPRESSION), true);
        try {
            dos.write(jsonData.getBytes());
        } finally {
            dos.flush();
            dos.close();
        }
        return baos.toByteArray();
    }


    private String unCompressData(byte[] data) throws IOException {
        InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(data));
        StringWriter sw = new StringWriter(dataSize);
        IOUtils.copy(iis, sw);
        return sw.toString();
    }
}
