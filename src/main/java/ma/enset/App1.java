package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TP_ventes").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Chargement du fichier ventes.txt
        JavaRDD<String> rddLines = sc.textFile("ventes.txt");

        // Extraction de la colonne des villes
        JavaRDD<String> rddVentes = rddLines.map(line -> line.split(",")[1].trim());

        // Création des paires (ville, 1) pour compter les occurrences de chaque ville
        JavaPairRDD<String, Integer> rddPairVille = rddVentes.mapToPair(ville -> new Tuple2<>(ville, 1));

        // Réduction par clé pour obtenir le nombre d'occurrences de chaque ville
        JavaPairRDD<String, Integer> rddCitiesCount = rddPairVille.reduceByKey(Integer::sum);

        // Affichage des résultats
        rddCitiesCount.foreach(elem -> System.out.println(elem._1() + " : " + elem._2()));
    }
}
