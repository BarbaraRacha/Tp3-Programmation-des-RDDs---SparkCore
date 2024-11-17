package ma.enset;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

public class App2 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("TP_ventes") // important de la définir
                .setMaster("local[*]"); // définit l'endroit de déploiement de l'App (mode local, cluster spark, Kubernetes, Cloud ..)
        //local[*] : * définit le nbre de thread à utiliser par l'app
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Chargement du fichier ventes.txt
        JavaRDD<String> rddLines = sc.textFile("ventes.txt");

        // Création des paires (ville, prix) en extrayant à la fois la ville et le prix de chaque ligne
        JavaPairRDD<String, Integer> rddPairVillePrix = rddLines.mapToPair(line -> {
            String[] parts = line.split(",");
            String ville = parts[1].trim();  // Extraction de la ville
            int prix = Integer.parseInt(parts[3].trim().split(" ")[0]);  // Extraction du prix
            return new Tuple2<>(ville, prix);
        });

        // Réduction par clé (ville) pour obtenir la somme des prix par ville
        JavaPairRDD<String, Integer> rddTotalParVille = rddPairVillePrix.reduceByKey(Integer::sum);

        // Affichage des résultats
        rddTotalParVille.foreach(elem -> System.out.println(elem._1() + " : " + elem._2() + " MAD"));
    }
}
