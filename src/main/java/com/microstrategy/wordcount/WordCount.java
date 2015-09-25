package com.microstrategy.wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.kohsuke.args4j.CmdLineException;

import scala.Tuple2;

/**
 * @author Siqi Wu
 * 
 * Class implemented Spark wordcount function
 *
 */
public class WordCount 
{
	private static WordCountOptions options;
	
    public static void main( String[] args ) throws CmdLineException
    {
    	WordCount.options = new WordCountOptions();
    	WordCount.options.parse(args);
    	
    	SparkConf sparkConf = new SparkConf();
        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
    	
    	JavaRDD<String> textFile = sparkContext.textFile(WordCount.options.inputFile);
    	
    	JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
    	  public Iterable<String> call(String s) { return Arrays.asList(s.split(" ")); }
    	});
    	
    	JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
    	  public Tuple2<String, Integer> call(String s) { return new Tuple2<String, Integer>(s, 1); }
    	});
    	
    	JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
    	  public Integer call(Integer a, Integer b) { return a + b; }
    	});
    	
    	counts.saveAsTextFile(WordCount.options.outputFile);
    	
    	sparkContext.close();
    }
}
