package org.example;

import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MachineLearning {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("project").getOrCreate();

        // burada getirilen veriseti kafka ile gelmeli
        Dataset<Row> loadData = sparkSession.read().format("csv")
                .option("header",true)
                .option("inferSchema",true)
                .load("C:\\Users\\aFurkan\\Desktop\\diyabet.csv");

        String[] headerList = {"Diabetes_binary","HighBP","HighChol","CholCheck","BMI","Smoker","Stroke","HeartDiseaseorAttack","PhysActivity","Fruits"};

        List<String> headers = Arrays.asList(headerList);
        List<String> headersResult = new ArrayList<String>();
        for(String h:headers) {
            if (h.equals("Diabetes_binary")) {
                StringIndexer indexTmp = new StringIndexer().setInputCol(h).setOutputCol("label");
                loadData = indexTmp.fit(loadData).transform(loadData);
                headersResult.add("label");
            } else {
                StringIndexer indexTmp = new StringIndexer().setInputCol(h).setOutputCol(h.toLowerCase() + "_cat");
                loadData = indexTmp.fit(loadData).transform(loadData);
                headersResult.add(h.toLowerCase() + "_cat");
            }
        }
        //loadData.show();
        String[] colList = headersResult.toArray(new String[headersResult.size()]);
        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(colList).setOutputCol("features"); // vektörize edildi

        // veriler vektörize edildi
        Dataset<Row> transform_data = vectorAssembler.transform(loadData);

        // yeni veri tablosu oluşturuldu
        Dataset<Row> final_data = transform_data.select("label","features");

        // makine öğrenmesi //

        // test ve train datasetler oluşturuluyor
        Dataset<Row>[] datasets = final_data.randomSplit(new double[]{0.7,0.3});
        Dataset<Row> train_data = datasets[0];
        Dataset<Row> test_data = datasets[1];

        // naive bayes ile model eğitildi
        NaiveBayes nb = new NaiveBayes();
        nb.setSmoothing(1);
        NaiveBayesModel model = nb.fit(train_data);

        // model test edildi
        Dataset<Row> predictions = model.transform(test_data);

        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");

        double evaluate = evaluator.evaluate(predictions);

        System.out.println("Accuracy: " + evaluate);

        // predictions.show();

        //final_data.show();
    }
}
