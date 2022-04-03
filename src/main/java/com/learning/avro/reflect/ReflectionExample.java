package com.learning.avro.reflect;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;

public class ReflectionExample {

    public static void main(String[] args) {

        // using reflection to get the schema
        Schema schema = ReflectData.get().getSchema(ReflectedCustomer.class);
        System.out.println("Schema = " + schema.toString(true));


        // write to file
        final DatumWriter<ReflectedCustomer> datumWriter = new ReflectDatumWriter<>(ReflectedCustomer.class);
        try (DataFileWriter<ReflectedCustomer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.setCodec(CodecFactory.deflateCodec(9)).create(schema, new File("customer-reflected.avro"));
            dataFileWriter.append(new ReflectedCustomer("Bill", "Clark", "The Rocket"));
            System.out.println("Successfully wrote customer-reflected.avro");
        } catch (IOException e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }

        // read from file
        final File file = new File("customer-reflected.avro");
        final DatumReader<ReflectedCustomer> datumReader = new ReflectDatumReader<>(ReflectedCustomer.class);
        try (DataFileReader<ReflectedCustomer> dataFileReader = new DataFileReader<>(file, datumReader)) {
            for (ReflectedCustomer reflectedCustomer : dataFileReader) {
                System.out.println(reflectedCustomer.toString());
                System.out.println("Full name: " + reflectedCustomer.getFullName());
            }
            System.out.println("Successfully read avro file");
        } catch (IOException e) {
            e.printStackTrace();
        }



    }
}
