package com.company.examples.types;


import com.company.examples.types.serde.JsonSerializer;
import com.company.examples.types.product;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.*;
import java.util.List;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";
        String topic = "product";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        // create the producer
        KafkaProducer<String, product> producer = new KafkaProducer<String, product>(properties);
        product p1 = new product();
   try {
  
       //reading data from json file 
       ObjectMapper mapper = new ObjectMapper();
       InputStream inputstream = new FileInputStream(new File("product.json"));
       TypeReference<List<product>> typeReference = new TypeReference<List<product>>() { };
       List<product> prod= mapper.readValue(inputstream,typeReference);

     // we can send random data as below in comments

     /*  p1.withPogId("1");
       p1.withBrand("Apple");
       p1.withCategory("c1");
       p1.withCountry("India");
       p1.withDescription("electronics");
       p1.withCreationtime("friday");
       p1.withPrice("10000");
       p1.withQuantity("3");
       p1.withSize("NA");
       p1.withStock("4");
       p1.withSubcategory("Iphone");
       p1.withSupc("AA");
       p1.withSellercode("qwerty"); */

       //sending json data as list of product object form using for loop
       for(product p : prod) {



           ProducerRecord<String, product> record =
                   new ProducerRecord<String, product>(topic, p.getPogId(), p);

           // send data - asynchronous
           producer.send(record);

           // flush data
           producer.flush();
           // flush and close producer
           producer.close();

       }
   } catch (FileNotFoundException e) {
       e.printStackTrace();
   } catch (JsonParseException e) {
       e.printStackTrace();
   } catch (JsonMappingException e) {
       e.printStackTrace();
   } catch (IOException e) {
       e.printStackTrace();
   }





    }
}
