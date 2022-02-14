package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

import java.util.ArrayList;
import java.util.List;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0];
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		// Create a configuration object and set the name of the application
//		SparkConf conf = new SparkConf().setAppName("Spark Lab #7");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		 SparkConf conf=new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		//** START **//
		JavaRDD<String> registerRDD = sc.textFile(inputPath);
		JavaRDD<String> stationsRDD = sc.textFile(inputPath2);

		//remove header and rows with userSlot = freeSlot = 0
		JavaRDD<String> registerRDDfilt = registerRDD.filter(el->{
			String[] splitted = el.split("\\t+");
			if(splitted[0].equals("station"))
				return false;
			int used_slot = Integer.parseInt(splitted[2]);
			int free_slot = Integer.parseInt(splitted[3]);
			if(free_slot==0 && used_slot ==0)
				return false;
			return true;
		});
		System.out.println("total lines of readings after filter : "+registerRDDfilt.count());
		//remove header from stations
		JavaRDD<String> stationsRDDfilt = stationsRDD.filter(el->{
			return !el.startsWith("id");
		});

		// create PairRdd with:
		// KEY   : "id_station;timeslot" where timeslot = weekday-hour es: "wed-15"
		// VALUE : Obj SlotReading(stationId, tot_reading, tot_critical_reading) where critical is when free_slot==0
		JavaPairRDD<String, SlotReading> stationTimeslotReadingPairRDD = registerRDDfilt.mapToPair(el -> {
			String[] splitted = el.split("\\t+");
			double total_reading=1; // always +1 for total reading (later summed by reduceByKey)
			double critical_reading = Double.parseDouble(splitted[3])==0 ? 1 : 0; //1 if critical, else 0
			String stationId = splitted[0];
			String weekday = DateTool.DayOfTheWeek(splitted[1]);
			String hour = splitted[1].replaceAll(":.*", "").split("\\s")[1]; //tak the 'hour' value
			String timeSlot = weekday+"-"+hour;
			return new Tuple2<>(
					stationId+";"+timeSlot,
					new SlotReading(Integer.parseInt(stationId), total_reading, critical_reading, splitted[1])
			);
		});
		System.out.println(stationTimeslotReadingPairRDD.take(10));
		JavaPairRDD<String, SlotReading> stationTimeslotReadingPairRDDreduced = stationTimeslotReadingPairRDD.reduceByKey(
				(el1, el2)->{
					return new SlotReading(
							el1.getStationId(),
							el1.getTotalReading()+el2.getTotalReading(),
							el1.getCriticalReading()+el2.getCriticalReading()
					);
				}
		);
		System.out.println("\n");
		System.out.println(stationTimeslotReadingPairRDDreduced.collect());

		// iterate over values with format
		// KEY   : 1;Thu - 12
		// VALUE : SlotReading{stationId=1, totalReading=1, criticalReading=0}
		// calculate criticality and return those over the threshold
		JavaRDD<String> resultKML = stationTimeslotReadingPairRDDreduced.flatMap(el->{
			List<String> selected = new ArrayList<String>();

			double percentage = el._2().getCriticalReading()/el._2().getTotalReading();
			System.out.println(percentage);

			//if it's over the treshold, compose the KML
			if(percentage>threshold){
				int stationId = el._2().getStationId();
				// el._01 = "1;Thu-12"
				String weekDay = el._1().split(";")[1].split("-")[0];
				String hour = el._1().split(";")[1].split("-")[1];
				String coordinates = "fake";


				String result = "<Placemark><name>" + stationId + "</name>" + "<ExtendedData>"
						+ "<Data name=\"DayWeek\"><value>" + weekDay + "</value></Data>"
						+ "<Data name=\"Hour\"><value>" + hour + "</value></Data>"
						+ "<Data name=\"Criticality\"><value>" + percentage + "</value></Data>"
						+ "</ExtendedData>" + "<Point>" + "<coordinates>" + coordinates + "</coordinates>"
						+ "</Point>" + "</Placemark>";

				System.out.println("\nFirst param of selected");
				System.out.println(el._1());
				System.out.println("Second param of selected");
				System.out.println(el._2());
				selected.add(result);
			}


			return selected.iterator();
		});







		// Store in resultKML one String, representing a KML marker, for each station 
		// with a critical timeslot 
		// JavaRDD<String> resultKML = .....
		  
		// Invoke coalesce(1) to store all data inside one single partition/i.e., in one single output part file
		resultKML.coalesce(1).saveAsTextFile(outputFolder);

		// Close the Spark context
		sc.close();
	}
}
