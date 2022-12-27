package edu.upenn.cis.nets2120.hw3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.api.java.function.PairFunction;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceInUseException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import edu.upenn.cis.nets2120.config.Config;
import edu.upenn.cis.nets2120.storage.DynamoConnector;
import scala.Tuple2;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import opennlp.tools.stemmer.PorterStemmer;
import opennlp.tools.stemmer.Stemmer;
import opennlp.tools.tokenize.SimpleTokenizer;

public class LoadNetwork {
	/**
	 * The basic logger
	 */
	static Logger logger = LogManager.getLogger(LoadNetwork.class);

	/**
	 * Connection to DynamoDB
	 */
	DynamoDB db;
	Table articles;
	Table inverted;
	SimpleTokenizer model;
	Stemmer stemmer;
	JSONParser parser;

	/**
	 * Helper function: swap key and value in a JavaPairRDD
	 * 
	 * @author zives
	 *
	 */
	static class SwapKeyValue<T1, T2> implements PairFunction<Tuple2<T1, T2>, T2, T1> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<T2, T1> call(Tuple2<T1, T2> t) throws Exception {
			return new Tuple2<>(t._2, t._1);
		}

	}

	public LoadNetwork() {
		System.setProperty("file.encoding", "UTF-8");
		model = SimpleTokenizer.INSTANCE;
		stemmer = new PorterStemmer();
	}

	private void initializeTables() throws DynamoDbException, InterruptedException {
		try {
			articles = db.createTable("articles", Arrays.asList(new KeySchemaElement("id", KeyType.HASH)), // Partition
																												// key
					Arrays.asList(new AttributeDefinition("id", ScalarAttributeType.N)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier.
			articles.waitForActive();
		} catch (final ResourceInUseException exists) {
			articles = db.getTable("articles");
		}
		
		try {
			inverted = db.createTable("inverted", Arrays.asList(new KeySchemaElement("keyword", KeyType.HASH),
					new KeySchemaElement("articleId", KeyType.RANGE)),
					Arrays.asList(new AttributeDefinition("keyword", ScalarAttributeType.S),
							new AttributeDefinition("articleId", ScalarAttributeType.N)),
					new ProvisionedThroughput(25L, 25L)); // Stay within the free tier

			inverted.waitForActive();
		} catch (final ResourceInUseException exists) {
			inverted = db.getTable("inverted");
		}
	}

	/**
	 * Initialize the database connection and open the file
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws DynamoDbException
	 */
	public void initialize() throws IOException, DynamoDbException, InterruptedException {
		logger.info("Connecting to DynamoDB...");
		db = DynamoConnector.getConnection(Config.DYNAMODB_URL);

		initializeTables();
		
		logger.debug("Connected!");
	}
	
	public void run() {
		parser = new JSONParser();
		ArrayList<JSONObject> articleJsons = new ArrayList<>();
		BufferedReader fil = null;
		String line = null;
		
		try {
			fil = new BufferedReader(new FileReader(new File(Config.NEWS_FEED_PATH)));
			do {
				try {
					line = fil.readLine();
					if (line != null) {
						JSONObject obj = (JSONObject) parser.parse(line);
						articleJsons.add(obj);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} while (line != null);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		List<Item> keywordsBatch = new ArrayList<>();
		List<Item> articlesBatch = new ArrayList<>();
		List<String> stopWords = Arrays.asList(new String[] {"a", "all", "any", "but", "the"});
		
		for (int i = 0; i < articleJsons.size(); i++) {
			Set<String> words = new HashSet<>();
			JSONObject object = articleJsons.get(i);
			String date = (String) object.get("date");
			String updatedYear = Integer.toString(Integer.valueOf(date.substring(0, 4)) + 5);
			String updatedDate = updatedYear + date.substring(4);
			
			articlesBatch.add(new Item().withPrimaryKey("id", i)
					.withString("url", (String) object.get("link"))
					.withString("headline", (String) object.get("headline"))
					.withString("category", (String) object.get("category"))
					.withString("authors", (String) object.get("authors"))
					.withString("short_description", (String) object.get("short_description"))
					.withString("date", updatedDate)
					.withString("proxy", "y"));
			
			String[] tokenized = model.tokenize((String) object.get("headline") + " " +
					(String) object.get("category") + " " + (String) object.get("authors") + " " +
					(String) object.get("short_description"));
			for (int j = 0; j < tokenized.length; j++) {
				String word = tokenized[j];
				//Check if the word is all alphabets
				if (word.chars().allMatch(Character::isLetter)) {
					//Convert to lower case
					word = word.toLowerCase();
					//Check if the word is not a stop word
					if (!stopWords.contains(word)) {
						//Stem the word
						word = (String) stemmer.stem(word);
						//Checking the word hasn't been added for this article
						if (!words.contains(word)) {
							//Adding the word to our set of words and adding the corresponding Item to the batch
							words.add(word);
							keywordsBatch.add(new Item().withPrimaryKey("keyword", word, "articleId", i)
									.withString("keyword", word)
									.withNumber("articleId", i));
						}
					}
				}
				//If the batch is already full for this column, call the helper
				if (keywordsBatch.size() == 25) {
					writer(keywordsBatch, "inverted");
					keywordsBatch = new ArrayList<>();
				}
			}
			//Calling the helper if the batch is full
			if (keywordsBatch.size() == 25) {
				writer(keywordsBatch, "inverted");
				keywordsBatch = new ArrayList<>();
			}
			if (articlesBatch.size() == 25) {
				writer(articlesBatch, "articles");
				articlesBatch = new ArrayList<>();
			}
		}
		//Calling the helper at the end
		if (keywordsBatch.size() >  0) {
			writer(keywordsBatch, "inverted");
		}
		if (articlesBatch.size() > 0) {
			writer(articlesBatch, "articles");
		}
		
	}
	
	//Helper method with a batch argument that conducts batchWriteItem and handles unprocessed items
	private void writer(List<Item> batch, String tableName) {
		try {
			TableWriteItems tableWriteItems = new TableWriteItems(tableName).withItemsToPut(batch);
			BatchWriteItemOutcome outcome = db.batchWriteItem(tableWriteItems);
			do {
				Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
				if (unprocessedItems.size() != 0) {
					outcome = db.batchWriteItemUnprocessed(unprocessedItems);
				}
			} while (outcome.getUnprocessedItems().size() > 0);
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}

	
	/**
	 * Graceful shutdown
	 */
	public void shutdown() {
		logger.info("Shutting down");

		DynamoConnector.shutdown();
	}

	public static void main(String[] args) {
		final LoadNetwork ln = new LoadNetwork();

		try {
			ln.initialize();
			ln.run();
		} catch (final IOException ie) {
			logger.error("I/O error: ");
			ie.printStackTrace();
		} catch (final DynamoDbException e) {
			e.printStackTrace();
		} catch (final InterruptedException e) {
			e.printStackTrace();
		} finally {
			ln.shutdown();
		}
	}

}
