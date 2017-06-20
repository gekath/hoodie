/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.common;

import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodiePartitionMetadata;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordLocation;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Class to be used in tests to keep generating test inserts and updates against a corpus.
 *
 * Test data uses a toy Uber trips, data model.
 */
public class HoodieTestDataGenerator {
    static class KeyPartition {
        HoodieKey key;
        String partitionPath;
    }

    public static String TRIP_EXAMPLE_SCHEMA = "{\"type\": \"record\","
            + "\"name\": \"triprec\","
            + "\"fields\": [ "
            + "{\"name\": \"timestamp\",\"type\": \"double\"},"
            + "{\"name\": \"_row_key\", \"type\": \"string\"},"
            + "{\"name\": \"rider\", \"type\": \"string\"},"
            + "{\"name\": \"driver\", \"type\": \"string\"},"
            + "{\"name\": \"begin_lat\", \"type\": \"double\"},"
            + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
            + "{\"name\": \"end_lat\", \"type\": \"double\"},"
            + "{\"name\": \"end_lon\", \"type\": \"double\"},"
            + "{\"name\":\"fare\",\"type\": \"double\"}]}";

    public static String TRIP_EXAMPLE_SCHEMA_ADD_ONE = "{\"type\": \"record\","
            + "\"name\": \"triprec\","
            + "\"fields\": [ "
            + "{\"name\": \"timestamp\",\"type\": \"double\"},"
            + "{\"name\": \"_row_key\", \"type\": \"string\"},"
            + "{\"name\": \"rider\", \"type\": \"string\"},"
            + "{\"name\": \"driver\", \"type\": \"string\"},"
            + "{\"name\": \"begin_lat\", \"type\": \"double\"},"
            + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
            + "{\"name\": \"end_lat\", \"type\": \"double\"},"
            + "{\"name\": \"end_lon\", \"type\": \"double\"},"
            + "{\"name\": \"car_num\", \"type\": [\"null\", \"double\"], \"default\": \"null\"},"
            + "{\"name\":\"fare\",\"type\": \"double\"}]}";

    public static String TRIP_EXAMPLE_SCHEMA_ADD_TWO = "{\"type\": \"record\","
            + "\"name\": \"triprec\","
            + "\"fields\": [ "
            + "{\"name\": \"timestamp\",\"type\": \"double\"},"
            + "{\"name\": \"_row_key\", \"type\": \"string\"},"
            + "{\"name\": \"rider\", \"type\": \"string\"},"
            + "{\"name\": \"driver\", \"type\": \"string\"},"
            + "{\"name\": \"begin_lat\", \"type\": \"double\"},"
            + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
            + "{\"name\": \"end_lat\", \"type\": \"double\"},"
            + "{\"name\": \"end_lon\", \"type\": \"double\"},"
            + "{\"name\": \"car_num\", \"type\": [\"null\", \"double\"], \"default\": \"null\"},"
            + "{\"name\": \"car_model\", \"type\": [\"null\", \"string\"], \"default\": \"null\"},"
            + "{\"name\":\"fare\",\"type\": \"double\"}]}";

    public static String TRIP_EXAMPLE_SCHEMA_ADD_THREE = "{\"type\": \"record\","
            + "\"name\": \"triprec\","
            + "\"fields\": [ "
            + "{\"name\": \"timestamp\",\"type\": \"double\"},"
            + "{\"name\": \"_row_key\", \"type\": \"string\"},"
            + "{\"name\": \"rider\", \"type\": \"string\"},"
            + "{\"name\": \"driver\", \"type\": \"string\"},"
            + "{\"name\": \"begin_lat\", \"type\": \"double\"},"
            + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
            + "{\"name\": \"end_lat\", \"type\": \"double\"},"
            + "{\"name\": \"end_lon\", \"type\": \"double\"},"
            + "{\"name\": \"car_num\", \"type\": [\"null\", \"double\"], \"default\": \"null\"},"
            + "{\"name\": \"car_model\", \"type\": [\"null\", \"string\"], \"default\": \"null\"},"
            + "{\"name\": \"car_color\", \"type\": [\"null\", \"string\"], \"default\": \"null\"},"
            + "{\"name\":\"fare\",\"type\": \"double\"}]}";

    public static String TRIP_EXAMPLE_SCHEMA_ADD_COLLISION = "{\"type\": \"record\","
            + "\"name\": \"triprec\","
            + "\"fields\": [ "
            + "{\"name\": \"timestamp\",\"type\": \"double\"},"
            + "{\"name\": \"_row_key\", \"type\": \"string\"},"
            + "{\"name\": \"rider\", \"type\": \"string\"},"
            + "{\"name\": \"driver\", \"type\": \"string\"},"
            + "{\"name\": \"begin_lat\", \"type\": \"double\"},"
            + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
            + "{\"name\": \"end_lat\", \"type\": \"double\"},"
            + "{\"name\": \"end_lon\", \"type\": \"double\"},"
            + "{\"name\": \"car_num\", \"type\": [\"null\", \"string\"], \"default\": \"null\"},"
            + "{\"name\":\"fare\",\"type\": \"double\"}]}";


    // based on examination of sample file, the schema produces the following per record size
    public static final int SIZE_PER_RECORD = 50 * 1024;

    public static final String[] DEFAULT_PARTITION_PATHS = {"2016/03/15", "2015/03/16", "2015/03/17"};

    public static boolean field_one = false;
    public static boolean field_two = false;
    public static boolean field_three = false;
    public static boolean field_four = false;

    private static Logger logger = LogManager.getLogger(HoodieTestDataGenerator.class);
    public transient SQLContext sqlContext;

    public static void writePartitionMetadata(FileSystem fs, String[] partitionPaths, String basePath) {
        for (String partitionPath: partitionPaths) {
            new HoodiePartitionMetadata(fs, "000", new Path(basePath), new Path(basePath, partitionPath)).trySave(0);
        }
    }

    private List<KeyPartition> existingKeysList = new ArrayList<>();
    public static Schema avroSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(TRIP_EXAMPLE_SCHEMA));
    private static Random rand = new Random(46474747);
    private String[] partitionPaths = DEFAULT_PARTITION_PATHS;

    public HoodieTestDataGenerator(String[] partitionPaths) {
        this.partitionPaths = partitionPaths;
    }

    public HoodieTestDataGenerator() {
        this(new String[]{"2016/03/15", "2015/03/16", "2015/03/17"});
    }

    public HoodieTestDataGenerator(SQLContext sqlContext, String[] partitionPaths, String schema) {
        this.sqlContext = sqlContext;
        this.partitionPaths = partitionPaths;
        setSchema(schema);
    }


    public void setSchema(String schema) {
        avroSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(schema));
    }


    /**
     * Generates new inserts, uniformly across the partition paths above. It also updates the list
     * of existing keys.
     */
    public List<HoodieRecord> generateInserts(String commitTime, int n) throws IOException {
        List<HoodieRecord> inserts = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            String partitionPath = partitionPaths[rand.nextInt(partitionPaths.length)];
            HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
            HoodieRecord record = new HoodieRecord(key, generateRandomValue(key, commitTime));
            inserts.add(record);

            KeyPartition kp = new KeyPartition();
            kp.key = key;
            kp.partitionPath = partitionPath;
            existingKeysList.add(kp);
        }
        return inserts;
    }

    public List<HoodieRecord> generateDeletes(String commitTime, int n) throws IOException {
        List<HoodieRecord> inserts = generateInserts(commitTime, n);
        return generateDeletesFromExistingRecords(inserts);
    }

    public List<HoodieRecord> generateDeletesFromExistingRecords(List<HoodieRecord> existingRecords) throws IOException {
        List<HoodieRecord> deletes = new ArrayList<>();
        for (HoodieRecord existingRecord: existingRecords) {
            HoodieRecord record = generateDeleteRecord(existingRecord);
            deletes.add(record);

        }
        return deletes;
    }

    public HoodieRecord generateDeleteRecord(HoodieRecord existingRecord) throws IOException  {
        HoodieKey key = existingRecord.getKey();
        TestRawTripPayload payload = new TestRawTripPayload(Optional.empty(), key.getRecordKey(), key.getPartitionPath(), null, true);
        return new HoodieRecord(key, payload);
    }

    public List<HoodieRecord> generateUpdates(String commitTime, List<HoodieRecord> baseRecords) throws IOException {
        List<HoodieRecord> updates = new ArrayList<>();
        for (HoodieRecord baseRecord: baseRecords) {
            HoodieRecord record = new HoodieRecord(baseRecord.getKey(), generateRandomValue(baseRecord.getKey(), commitTime));
            updates.add(record);
        }
        return updates;
    }

    /**
     * Generates new updates, randomly distributed across the keys above.
     */
    public List<HoodieRecord> generateUpdates(String commitTime, int n) throws IOException {
        List<HoodieRecord> updates = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            KeyPartition kp = existingKeysList.get(rand.nextInt(existingKeysList.size() - 1));
            HoodieRecord record = new HoodieRecord(kp.key, generateRandomValue(kp.key, commitTime));
            updates.add(record);
        }
        return updates;
    }


    /**
     * Generates a new avro record of the above schema format, retaining the key if optionally
     * provided.
     */
    public HoodieRowPayload generateRandomValue(HoodieKey key, String commitTime) throws IOException {
        GenericRecord rec = generateGenericRecord(key.getRecordKey(), "rider-" + commitTime,
            "driver-" + commitTime, 0.0);
        HoodieAvroUtils.addCommitMetadataToRecord(rec, commitTime, "-1");

        List<String> jsonData = Arrays.asList(rec.toString());
        JavaRDD<String> rddData = new JavaSparkContext(sqlContext.sparkContext()).parallelize(jsonData);
        Dataset<Row> finalData = sqlContext.read().json(rddData);
        return new HoodieRowPayload(finalData.first());
    }

    public static GenericRecord generateGenericRecord(String rowKey, String riderName,
        String driverName, double timestamp) {
        GenericRecord rec = new GenericData.Record(avroSchema);
        rec.put("_row_key", rowKey);
        rec.put("timestamp", timestamp);
        rec.put("rider", riderName);
        rec.put("driver", driverName);
        rec.put("begin_lat", rand.nextDouble());
        rec.put("begin_lon", rand.nextDouble());
        rec.put("end_lat", rand.nextDouble());
        rec.put("end_lon", rand.nextDouble());
        rec.put("fare", rand.nextDouble() * 100);
        if (field_one) {
            rec.put("car_num", rand.nextInt(100));
        }
        if (field_two) {
            rec.put("car_model", "model-" + String.valueOf(rand.nextInt(100)));
        }
        if (field_three) {
            rec.put("car_color", "black");
        }
        if (field_four) {
            rec.put("car_num", "black");
        }
        return rec;
    }

    public static void createCommitFile(String basePath, String commitTime) throws IOException {
        Path commitFile =
            new Path(basePath + "/" + HoodieTableMetaClient.METAFOLDER_NAME + "/" + HoodieTimeline.makeCommitFileName(commitTime));
        FileSystem fs = FSUtils.getFs();
        FSDataOutputStream os = fs.create(commitFile, true);
        HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
        try {
            // Write empty commit metadata
            os.writeBytes(new String(commitMetadata.toJsonString().getBytes(
                StandardCharsets.UTF_8)));
        } finally {
            os.close();
        }

    }

    public String[] getPartitionPaths() {
        return partitionPaths;
    }

    public void setFieldOne(boolean is_field) {
        this.field_one = is_field;
    }

    public void setFieldTwo(boolean is_field) {
        this.field_two = is_field;
    }

    public void setFieldThree(boolean is_field) {
        this.field_three = is_field;
    }

    public void setFieldFour(boolean is_field) {
        this.field_four = is_field;
    }
}
