package edu.upenn.cis.nets2120.hw3.livy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

public class AdsorptionLivy {
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ExecutionException {
		
		LivyClient client = new LivyClientBuilder()
				  .setURI(new URI("http://ec2-3-238-143-240.compute-1.amazonaws.com:8998/"))
				  .build();

		try {
			String jar = "target/nets2120-hw3-0.0.1-SNAPSHOT.jar";
			System.out.printf("Uploading %s to the Spark context...\n", jar);
			client.uploadJar(new File(jar)).get();
			client.submit(new AdsorptionJob()).get();
			System.out.println("Job Completed");
		} finally {
			client.stop(true);
		}
	}

}
