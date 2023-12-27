package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.Scanner;
import java.util.UUID;
import java.util.stream.Stream;

public class ProjeProducer {
    private static String port = "localhost:9092";
    private static String KafkaTopic = "diabet";

    private static Producer<String, String> ProducerProperties(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, port);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws URISyntaxException {
        Producer<String, String> csvProducer = ProducerProperties();

        Scanner read = new Scanner(System.in);
        while(true){
            System.out.println("gir: ");
            String key = read.nextLine();
            String value = "  gelen deger  ";
            ProducerRecord<String,String> rec = new ProducerRecord<String,String>(KafkaTopic,key,value);
            csvProducer.send(rec);
        }

//        try{
//            Path path = Paths.get("C:\\Users\\aFurkan\\Desktop\\diyabet.csv");
//            Stream<String> FileStream = Files.lines(path);
//
//            FileStream.forEach(line -> {
//                System.out.println(line);
//
//                final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
//                        KafkaTopic, UUID.randomUUID().toString(), line);
//
//                csvProducer.send(csvRecord);
//            });
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

//          System.out.println("*** Produce Islemi Tamamlandi ***");




/*
        try (CSVReader csvReader = new CSVReader(new FileReader("C:\\Users\\aFurkan\\Desktop\\diabetKucuk.csv"))) {
            String[] line;
            while ((line = csvReader.readNext()) != null) {
                // CSV satırını Kafka'ya gönder
                String key = line[0]; // Varsa bir anahtar belirleyebilirsiniz
                String value = String.join(",", line); // CSV satırını birleştirerek değeri oluştur
                ProducerRecord<String, String> record = new ProducerRecord<>("proje1", key, value);
                // Mesajı gönder
                producer.send(record);
                System.out.println(record);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
*/
    }
}