package org.sparkminds;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.Scanner;

public class SparkFirstProgram {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SparkFirstProgram")
                .master("local[*]")
                .getOrCreate();
        try (JavaSparkContext sc = new JavaSparkContext(sparkSession.sparkContext())) {
            JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
            List<Tuple2<Long, String>> results = initialRDD
                    .map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]", "")
                                             .toLowerCase())
                    .flatMap(sentence -> List.of(sentence.split("\\s+")).iterator())
                    .filter(sentence -> !sentence.trim().isBlank())
                    .filter(Util::isNotBoring)
                    .mapToPair(word -> new Tuple2<>(word, 1L))
                    .reduceByKey(Long::sum)
                    .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                    .sortByKey(false)
                    .take(10);
            results.forEach(System.out::println);
            try(Scanner scanner = new Scanner(System.in)) {
                System.out.println("Press enter to finish");
                scanner.nextLine();
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}