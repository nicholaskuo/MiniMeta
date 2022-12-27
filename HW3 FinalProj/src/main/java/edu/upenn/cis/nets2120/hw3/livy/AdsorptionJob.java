package edu.upenn.cis.nets2120.hw3.livy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Date;
import java.text.SimpleDateFormat;
import org.apache.livy.Job;
import org.apache.livy.JobContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import edu.upenn.cis.nets2120.storage.SparkConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;

public class AdsorptionJob implements Job<List<Integer>>  {
//public class AdsorptionJob {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;

	/**
	 * Connection to Apache Spark
	 */
	SparkSession spark;

	JavaSparkContext context;

	public AdsorptionJob() {
		System.setProperty("file.encoding", "UTF-8");
	}


	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws DynamoDbException
	 */
	public void initialize() throws IOException, InterruptedException {
		System.out.println("Connecting to DynamoDB...");
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);

		spark = SparkConnector.getSparkConnection();
		context = SparkConnector.getSparkContext();

		System.out.println("Connected!");
	}
	
	private JavaPairRDD<String, String> getPairRdd(String tableName) {
		
		ArrayList<Tuple2<String, String>> itemList = new ArrayList<>();
		Table table = db.getTable(tableName);	
		ItemCollection<ScanOutcome> items = table.scan();
		Iterator<Item> iterator = items.iterator();
		
		System.out.println("Scanned table: " + tableName);
		
		while (iterator.hasNext()) {
			
			Item item = iterator.next();

			if (tableName.equals("friends")) {
				itemList.add(new Tuple2<String, String>(
						(String) item.getString("username"),
						(String) item.getString("friend")));
			} else if (tableName.equals("articleLikes")) {
				itemList.add(new Tuple2<String, String>(
						(String) item.getString("username"),
						String.valueOf(item.getInt("articleId"))));
			} else if (tableName.equals("users")) {		
				List<String> categories = item.getList("categoryInterest");
				for (String category : categories) {
					itemList.add(new Tuple2<String, String>(
							(String) item.getString("username"),
							category));
				}
			} 
		}
		
		JavaPairRDD<String, String> result = context.parallelizePairs(itemList);
		System.out.println("Returning RDD");
		return result;
	}
	
	private JavaPairRDD<String, Tuple2<String, String>> getArticles() {
		ArrayList<Tuple2<String, Tuple2<String, String>>> itemList = new ArrayList<>();
		Table table = db.getTable("articles");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		Date date = new Date();
		String currDate = formatter.format(date);
		QuerySpec spec = new QuerySpec()
				.withKeyConditionExpression("proxy = :p and #d <= :today")
				.withNameMap(new NameMap().with("#d", "date"))
				.withValueMap(new ValueMap()
						.withString(":p", "y")
						.withString(":today", currDate));
		
		Index index = table.getIndex("proxy-date-index");
		ItemCollection<QueryOutcome> items = index.query(spec);
		Iterator<Item> iterator = items.iterator();
		
		while (iterator.hasNext()) {
			Item item = iterator.next();
			itemList.add(new Tuple2<String, Tuple2<String, String>>(
					String.valueOf(item.getInt("id")),
					new Tuple2<>((String) item.getString("category"),
							(String) item.getString("date"))));
		}
		
		return context.parallelizePairs(itemList);
	}
	
	private JavaPairRDD<Tuple2<String, String>, Double> getAdsorptionScores() {
		ArrayList<Tuple2<Tuple2<String, String>, Double>> itemList = new ArrayList<>();
		Table table = db.getTable("adsorptionScore");
		ItemCollection<ScanOutcome> items = table.scan();
		Iterator<Item> iterator = items.iterator();
		
		while (iterator.hasNext()) {
			Item item = iterator.next();
			System.out.print(item.toJSON());
			itemList.add(new Tuple2<Tuple2<String, String>, Double>(
					new Tuple2<>(String.valueOf(item.getInt("articleId")),
							item.getString("username")),
							item.getDouble("score")));
		}
		
		return context.parallelizePairs(itemList);
	}
	
	public void buildGraph(int iMax, double dMax) {
		
		System.out.println("Building Graph");
		
		JavaPairRDD<String, Tuple2<String, String>> articles = getArticles();
		JavaPairRDD<String, String> articleToCategory = articles.mapValues(t -> t._1);
		JavaPairRDD<String, String> categoryToArticle = articleToCategory
				.mapToPair(t -> new Tuple2<>(t._2, t._1));
		JavaPairRDD<String, String> friends = getPairRdd("friends");		
		JavaPairRDD<String, String> userToArticle = getPairRdd("articleLikes");
		JavaPairRDD<String, String> articleToUser = userToArticle
				.mapToPair(t -> new Tuple2<>(t._2, t._1));
		JavaPairRDD<String, String> userToCategory = getPairRdd("users");
		JavaPairRDD<String, String> categoryToUser = userToCategory
				.mapToPair(t -> new Tuple2<>(t._2, t._1));
		
		JavaPairRDD<String, Double> articleNodeTransfer = articleToCategory
				.union(articleToUser)
				.mapValues(toEdge -> 1.0)
				.reduceByKey((a, b) -> a + b)
				.mapValues(sum -> 1.0 / sum);
		
		JavaPairRDD<String, Double> categoryNodeTransfer = categoryToArticle
				.union(categoryToUser)
				.mapValues(toEdge -> 1.0)
				.reduceByKey((a, b) -> a + b)
				.mapValues(sum -> 1.0 / sum);
		
		JavaPairRDD<String, Double> userArticleNodeTransfer = userToArticle
				.mapValues(toEdge -> 1.0)
				.reduceByKey((a, b) -> a + b)
				.mapValues(sum -> 0.4 / sum);
		
		JavaPairRDD<String, Double> userCategoryNodeTransfer = userToCategory
				.mapValues(toEdge -> 1.0)
				.reduceByKey((a, b) -> a + b)
				.mapValues(sum -> 0.3 / sum);
		
		JavaPairRDD<String, Double> userFriendNodeTransfer = friends
				.mapValues(toEdge -> 1.0)
				.reduceByKey((a, b) -> a + b)
				.mapValues(sum -> 0.3 / sum);
		
		JavaPairRDD<String, Tuple2<String, Double>> articleCategoryEdgeTransfer = articleToCategory
				.join(articleNodeTransfer);
		
		JavaPairRDD<String, Tuple2<String, Double>> articleUserEdgeTransfer = articleToUser
				.join(articleNodeTransfer);
		
		JavaPairRDD<String, Tuple2<String, Double>> categoryArticleEdgeTransfer = categoryToArticle
				.join(categoryNodeTransfer);
		
		JavaPairRDD<String, Tuple2<String, Double>> categoryUserEdgeTransfer = categoryToUser
				.join(categoryNodeTransfer);
		
		JavaPairRDD<String, Tuple2<String, Double>> userFriendEdgeTransfer = friends
				.join(userFriendNodeTransfer);
		
		JavaPairRDD<String, Tuple2<String, Double>> userArticleEdgeTransfer = userToArticle
				.join(userArticleNodeTransfer);
		
		JavaPairRDD<String, Tuple2<String, Double>> userCategoryEdgeTransfer = userToCategory
				.join(userCategoryNodeTransfer);
		
		JavaPairRDD<String, Tuple2<String, Double>> edgeTransfer = articleCategoryEdgeTransfer
				.union(articleUserEdgeTransfer)
				.union(categoryArticleEdgeTransfer)
				.union(categoryUserEdgeTransfer)
				.union(userFriendEdgeTransfer)
				.union(userArticleEdgeTransfer)
				.union(userCategoryEdgeTransfer);
		
				JavaPairRDD<String, Tuple2<String, Double>> labelWeights = userToCategory
				.mapToPair(t -> new Tuple2<>(t._1, new Tuple2<>(t._1, 1.0)))
				.distinct();
		
		JavaPairRDD<String, Tuple2<String, Double>> initWeights = labelWeights;
		
		System.out.println("Running Adsorption");
		
		for (int i = 0; i < iMax; i++) {
			
			System.out.println("Iteration: " + i);
			
			if (i != 0) {
				labelWeights = labelWeights.union(initWeights);
			}
			
			JavaPairRDD<String, Tuple2<String, Double>> propagate = edgeTransfer
					.join(labelWeights)
					.mapToPair(t -> new Tuple2<String, Tuple2<String, Double>>(t._2._1._1,
							new Tuple2<>(t._2._2._1, t._2._1._2 * t._2._2._2)));
			
			JavaPairRDD<String, Tuple2<String, Double>> weightPerLabel = propagate
					.mapToPair(t -> new Tuple2<Tuple2<String, String>, Double>(
							new Tuple2<>(t._1, t._2._1), t._2._2))
					.reduceByKey((a, b) -> a + b)
					.mapToPair(t -> new Tuple2<String, Tuple2<String, Double>>(t._1._1,
							new Tuple2<>(t._1._2, t._2)));
			
			// weightPerLabel = weightPerLabel.filter(t -> t._2._2 > 0.000005);
			
			JavaPairRDD<String, Tuple2<String, Double>> labelAtNode = weightPerLabel
					.mapToPair(t -> new Tuple2<>(t._2._1, new Tuple2<>(t._1, t._2._2)));
			
			JavaPairRDD<String, Double> totalLabelWeight = labelAtNode
					.reduceByKey((a, b) -> new Tuple2<>(a._1, a._2 + b._2))
					.mapValues(t -> t._2);
			
			JavaPairRDD<String, Tuple2<String, Double>> normalizedLabelWeights = labelAtNode
					.join(totalLabelWeight)
					.mapToPair(t -> new Tuple2<>(t._2._1._1, new Tuple2<>(t._1, t._2._1._2 / t._2._2)));
			
			JavaPairRDD<Tuple2<String, String>, Double> tempLabels = labelWeights
					.mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1, t._2._1), t._2._2));
			
			JavaPairRDD<Tuple2<String, String>, Double> tempNormalizedLabels = normalizedLabelWeights
					.mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1, t._2._1), t._2._2));
			
			JavaPairRDD<Tuple2<String, String>, Double> deltaLabelWeights = tempLabels
					.join(tempNormalizedLabels)
					.mapValues(t -> Math.abs(t._1 - t._2))
					.filter(t -> t._2 > dMax);
			
			JavaPairRDD<Tuple2<String, String>, Double> deltaNewLabelWeights = tempNormalizedLabels
					.subtractByKey(tempLabels)
					.filter(t -> t._2 > dMax);

			labelWeights = normalizedLabelWeights;
			
			if (deltaLabelWeights.union(deltaNewLabelWeights).count() == 0) {
				break;
			}
		}
		
		labelWeights = labelWeights.filter(t -> {			
			if (t._1 == null) {
				return false;
			}
			try {
				Integer.valueOf(t._1);
			} catch (Exception e) {
				return false;
			}
			return true;
		});
		
		
		JavaPairRDD<String, Tuple2<String, Tuple2<Double, String>>> finalScores = labelWeights
				.join(articles)
				.mapValues(t -> new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2._2)));
		
		JavaPairRDD<Tuple2<String, String>, Tuple2<Double, String>> modifiedFinal = finalScores
				.mapToPair(t -> new Tuple2<>(new Tuple2<>(t._1, t._2._1), 
						new Tuple2<>(t._2._2._1, t._2._2._2)));
	
		JavaPairRDD<Tuple2<String, String>, Tuple2<Double, Tuple2<Double, String>>> oldScoresUnfiltered = 
				getAdsorptionScores().join(modifiedFinal);
		
		JavaPairRDD<Tuple2<String, String>, Tuple2<Double, String>> oldScores = oldScoresUnfiltered				
				.filter(t -> Math.abs(t._2._1 - t._2._2._1) > 0.00001)
				.mapValues(t -> t._2);
		
		modifiedFinal = modifiedFinal
				.subtractByKey(oldScoresUnfiltered)
				.union(oldScores);
		
		System.out.println("Writing to DynamoDB");
		
		modifiedFinal.foreachPartition(iter -> {
			//Local DynamoDB connecting inside lambda as connections are not serialized.
			Table adsorptionScore = DynamoConnector.getConnection(Config.DYNAMODB_URL).getTable("adsorptionScore");
			while (iter.hasNext()) {
				Tuple2<Tuple2<String, String>, Tuple2<Double, String>> tuple = iter.next();
				adsorptionScore.putItem(new Item()
						.withPrimaryKey("username", tuple._1._2, "articleId", Integer.valueOf(tuple._1._1))
						.withDouble("score", tuple._2._1)
						.withString("date", tuple._2._2));
			}
		});
	}
	
	public List<Integer> run() {
		buildGraph(30, 0.15);
		return new ArrayList<>();
	}

	
	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		System.out.println("Shutting down");

		DynamoConnector.shutdown();

		if (spark != null)
			spark.close();
	}
	
//	public static void main(String[] args) {
//		final AdsorptionJob ln = new AdsorptionJob();
//
//		try {
//			ln.initialize();
//			ln.run();
//		} catch (final IOException ie) {
//			ie.printStackTrace();
//		} catch (final DynamoDbException e) {
//			e.printStackTrace();
//		} catch (final InterruptedException e) {
//			e.printStackTrace();
//		} finally {
//			ln.shutdown();
//		}
//	}

	@Override
	public List<Integer> call(JobContext arg0) throws Exception {
		initialize();
		return run();
	}

}
