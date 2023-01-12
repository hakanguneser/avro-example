package com.avro.example;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;

public class Main {
public static void main(String[] args) throws IOException {

    File outputParquet = new File("./output.parquet");
    Files.deleteIfExists(outputParquet.toPath());
    Employee employee = new Employee("john", "john.doe@mail.com",BigDecimal.TEN);
    ParquetWriter<Employee> writer = AvroParquetWriter.<Employee>builder(new Path(outputParquet.getAbsolutePath()))
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withSchema(employee.getSchema())
            .build();
    try {
        writer.write(employee);
    } catch (Exception e) {
        e.printStackTrace();
    }
    writer.close();
}
}
