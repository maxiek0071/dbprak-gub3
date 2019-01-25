package gruppe1.gruppenuebung3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class Gruppenuebung3_Gruppe1 {

	public static void main(String[] args) {
		System.out.print("Setup: connecting to database..");
		EmbeddingRepository repo = EmbeddingRepository.createRepository("localhost", "5433", "postgres", "seLect14");
		
		
		if(repo != null) {
			System.out.println("SUCCESS");
			System.out.println();
			
			String srcFile = "src/main/resources/";
			
			
				// Import data to local database
				System.out.println();
				System.out.print("Importing embeddings into database... ");
				boolean importSuccess = false;
				try {
					importSuccess =  repo.importData(srcFile);
					System.out.println("SUCCESS");
				
				
				if(importSuccess) {
					System.out.println("SUCCESS");
					System.out.println();
					List<BenchmarkResult> results = new ArrayList<BenchmarkResult>();
					BenchmarkResult result = runBenchmark("BENCHMARk Word Similarty", new SimmilarityBenchmark("Word Similarty"), "src/main/resources/temporal_benchmark.txt", repo);
					if(result != null) {
						results.add(result);
					}
					result = runBenchmark("BENCHMARk Word Similarty Btree", new SimiliartyBTreeBenchmark("Word Similarty BTree"), "src/main/resources/temporal_benchmark.txt", repo);
					if(result != null) {
						results.add(result);
					}
					result = runBenchmark("BENCHMARk Word Similarty Hash", new SimiliartyHashBenchmark("Word Similarty Hash"), "src/main/resources/temporal_benchmark.txt", repo);
					if(result != null) {
						results.add(result);
					}
					
//					
//					BenchmarkResult	 result = runBenchmark("BENCHMARk: NeighborsBenchmark ", new NeighborsBenchmark("NeighborsBenchmark"), "src/main/resources/temporal_benchmark.txt", repo);
//					if(result != null) {
//						results.add(result);
//					}
//					
//					result = runBenchmark("BENCHMARK: Analogy with Gist ", new AnalogyGistBenchmark(), "src/main/resources/questions-words.txt", repo);
//					if(result != null) {
//						results.add(result);
//					}
//					
//					result = runBenchmark("BENCHMARK: Analogy with BTree ", new AnalogyBTreeBenchmark(), "src/main/resources/questions-words.txt", repo);
//					if(result != null) {
//						results.add(result);
//					}
					BenchmarkResultPrinter.printPerformance(results);
//					
//					
//
//					System.out.println("SUCCESS: You can view the results");
//				} else {
//					System.out.println("FAIL");
//					System.out.println("An error oucurred reading the data. Place the CSV files in the src/main/resources folder");
				}} 
				
			catch (IOException | SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Error: " + e.getMessage());
			} finally {
				repo.disconnect();
			}
			
		} else {
			System.out.println("FAIL");
			System.out.println("Error: Could not connect to database. ");
		}

	}
	
	
	private static BenchmarkResult runBenchmark(String bmName, Benchmark bm, String filePath, EmbeddingRepository repo) {
		BenchmarkResult result = null;
		System.out.println(bmName);								
		System.out.print("Importing Tasks..");
		boolean importSuccess = bm.importData(filePath);
		if (importSuccess) {
			System.out.println("SUCCESS");
			System.out.print("Running Tasks...");
			result= bm.run(repo);
			if (result != null) {
				System.out.println("SUCCESS");
			} else {
				System.out.println("FAIL");
			}
		} else {
			System.out.println("FAIL");
			System.out.println("Benchmark will be skipped due to error importing the tasks.");
			
		} 
		System.out.println();
		return result;
		
	}

}