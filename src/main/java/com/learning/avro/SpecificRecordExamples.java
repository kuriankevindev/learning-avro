package com.learning.avro;

import com.learning.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExamples {

    public static void main(String[] args) {

        // create specific record
        Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setFirstName("John");
        customerBuilder.setLastName("Doe");
        customerBuilder.setAge(25);
        customerBuilder.setHeight(170f);
        customerBuilder.setWeight(80.5f);
        customerBuilder.setAutomatedEmail(false);
        Customer customer = customerBuilder.build();
        System.out.println(customer);

        Customer.Builder customerBuilderWithDefault = Customer.newBuilder();
        customerBuilderWithDefault.setFirstName("John");
        customerBuilderWithDefault.setLastName("Doe");
        customerBuilderWithDefault.setAge(25);
        customerBuilderWithDefault.setHeight(170f);
        customerBuilderWithDefault.setWeight(80.5f);
        Customer customerWithDefault = customerBuilderWithDefault.build();
        System.out.println(customerWithDefault);

        // missing fields can cause runtime exception
        /* Customer.Builder customerBuilderMissingFields = Customer.newBuilder();
        customerBuilderMissingFields.setFirstName("John");
        customerBuilderMissingFields.setAge(25);
        customerBuilderMissingFields.setHeight(170f);
        customerBuilderMissingFields.setWeight(80.5f);
        Customer customerMissingFields = customerBuilderMissingFields.build();
        System.out.println(customerMissingFields); */

        // write to file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            dataFileWriter.append(customerWithDefault);
            System.out.println("Successfully wrote customer-specific.avro");
        } catch (IOException e){
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }

        // read from file
        final File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        try (DataFileReader<Customer> dataFileReader = new DataFileReader<>(file, datumReader)) {
            while (dataFileReader.hasNext()) {
                Customer readCustomer = dataFileReader.next();
                System.out.println(readCustomer.toString());
                System.out.println("First name: " + readCustomer.getFirstName());
            }
            System.out.println("Successfully read avro file");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
