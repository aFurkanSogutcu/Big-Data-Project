package org.example;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Consumer {

    public static void main(String[] args) {
        System.out.print("Spark Streaming Calisiyor...");

        SparkConf conf = new SparkConf().setAppName("proje").setMaster("local[*]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics = Collections.singleton("diabet");

        JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc, String.class,
                String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);


        directKafkaStream.foreachRDD(rdd -> {
            System.out.println("*** Güncellendi ***");
            if (rdd.count() > 0) {
                rdd.collect().forEach(record -> {
                    System.out.println(record);

                    // Makine Öğrenmesine Gidecek Veriler
                });
            }
        });

        /*
        List<String> allRecord = new ArrayList<String>();
        final String COMMA = ",";

        directKafkaStream.foreachRDD(rdd -> {

            System.out.println(" Yeni veri geldi ");
            if(rdd.count() > 0) {
                rdd.collect().forEach(rawRecord -> {

                    System.out.println(rawRecord);
                    System.out.println("***************************************");
                    System.out.println(rawRecord._2);
                    String record = rawRecord._2();
                    StringTokenizer st = new StringTokenizer(record,",");

                    StringBuilder sb = new StringBuilder();
                    while(st.hasMoreTokens()) {
                        String Diabetes_binary = st.nextToken();
                        String HighBP = st.nextToken();
                        String HighChol = st.nextToken();
                        String CholCheck = st.nextToken();
                        String BMI = st.nextToken();
                        String Smoker = st.nextToken();
                        String Stroke = st.nextToken();
                        String HeartDiseaseorAttack = st.nextToken();
                        String PhysActivity = st.nextToken();
                        String Fruits = st.nextToken();
                        String Veggies = st.nextToken();

                        sb.append(Diabetes_binary).append(COMMA).append(HighBP).append(COMMA).append(HighChol).append(COMMA).append(CholCheck).append(COMMA)
                        .append(BMI).append(COMMA).append(Smoker).append(COMMA).append(Stroke).append(COMMA).append(HeartDiseaseorAttack).append(COMMA)
                        .append(PhysActivity).append(COMMA).append(Fruits).append(COMMA).append(Veggies);
                        allRecord.add(sb.toString());
                    }
                });
                System.out.println("veri boyutu :"+allRecord.size());
                FileWriter writer = new FileWriter("diabetYeni.csv");
                for(String s : allRecord) {
                    writer.write(s);
                    writer.write("\n");
                }
                System.out.println("Master dataset has been created : ");
                writer.close();
            }
        });
*/

        ssc.start();
        ssc.awaitTermination();
    }
}
