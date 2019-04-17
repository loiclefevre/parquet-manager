package com.oracle.parquetmanager;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;

/**
 * Apache Parquet File Writer.
 * <p>
 * Converts data from CSV to Apache Parquet.
 * </p>
 *
 * @author <a href="mailto:loic.lefevre@oracle.com">loic.lefevre@oracle.com</a>
 */
public class ParquetFileWriter {
    public static void main(String[] args) {
        System.out.println("Apache Parquet Generator");

        if (args.length < 3) {
            System.out.println("Syntax: parquet-manager [schema file] [data file in CSV format] [output file in Parquet format]");
            System.exit(1);
        }

        final String schemaFile = args[0];

        final String sourceFile = args[1];

        final String destinationFile = args[2];

        System.out.println(". using schema    : " + schemaFile);
        System.out.println(". source file     : " + sourceFile);
        System.out.println(". destination file: " + destinationFile);

        final Schema schema = parseSchema(schemaFile);

        writeToParquet(schema, sourceFile, destinationFile);
    }

    private static Schema parseSchema(String schemaFile) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try (InputStream in = new FileInputStream(new File(schemaFile))) {
            // pass path to schema
            schema = parser.parse(in);

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return schema;
    }

    private static void writeToParquet(Schema schema, String sourceFile, String destinationFile) {
        // Path to Parquet file in HDFS
        System.setProperty("hadoop.home.dir", System.getenv("HADOOP_HOME"));

        try {
            final boolean couldDelete = new File(destinationFile).delete();
            if (!couldDelete) {
                System.err.println("Couldn't delete existing destination file!");
                System.exit(2);
            }
        } catch (Exception ignored) {
        }

        Path path = new Path(destinationFile);
        ParquetWriter<GenericData.Record> writer = null;

        // Creating ParquetWriter using builder
        try {
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .withValidation(false)
                    .withDictionaryEncoding(true)
                    .build();

            BufferedReader br = new BufferedReader(new FileReader(new File(sourceFile)), 1024 * 1024);

            String line;
            int lineNumber = 0;
            long start = System.currentTimeMillis();
            while ((line = br.readLine()) != null) {
                // skip first line
                if (lineNumber == 0) {
                    ++lineNumber;
                    continue;
                }

                if (lineNumber % 10000 == 0) {
                    System.out.println("Converted " + lineNumber + " rows so far (speed: 10,000 rows in " + (System.currentTimeMillis() - start) + " ms)");
                    start = System.currentTimeMillis();
                }

                try {
                    parseFieldsAndWriteRecord(line, writer, schema);
                } catch (Exception e) {
                    System.out.println("line #" + (lineNumber + 1) + ": " + line);
                    throw e;
                }

                //writer.write(Arrays.asList(fields));
                ++lineNumber;
            }

            br.close();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void parseFieldsAndWriteRecord(String line, ParquetWriter<GenericData.Record> writer, Schema schema) throws IOException {
        GenericData.Record record = new GenericData.Record(schema);

        int s;
        int e = -1;

        for (Schema.Field d : schema.getFields()) {
            s = e + 1;

            if (s < line.length()) {

                e = line.charAt(s) == '\"' ? line.indexOf(',', line.indexOf('\"', s + 1) + 1) : line.indexOf(',', s);

                //System.out.println(d);

                if (s < e) {
                    final String fieldName = d.name();
                    switch (d.schema().getType().getName()) {
                        case "int":
                            record.put(fieldName, Integer.parseInt(line.substring(s, e)));
                            break;
                        case "string":
                            record.put(fieldName, line.substring(s, e));
                            break;
                        case "double":
                            record.put(fieldName, Double.parseDouble(line.substring(s, e)));
                            break;
                        // for nullable fields
                        case "union":
                            switch (d.schema().getTypes().get(1).getType().getName()) {
                                case "int":
                                    record.put(fieldName, Integer.parseInt(line.substring(s, e)));
                                    break;
                                case "double":
                                    record.put(fieldName, Double.parseDouble(line.substring(s, e)));
                                    break;
                                case "string":
                                    record.put(fieldName, line.substring(s, e));
                                    break;
                                default:
                                    System.out.println("Not handled union data type: " + d.schema().getType().getName());
                                    break;
                            }
                            break;
                        default:
                            System.out.println("Not handled data type: " + d.schema().getType().getName());
                            break;
                    }
                }
            }
        }
        writer.write(record);
    }
}
