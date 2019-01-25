package gruppe1.gruppenuebung3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public class SimmilarityBenchmark extends Benchmark {

	public SimmilarityBenchmark(String name) {
		super(name);
	}
	
	@Override
	public boolean importData(String filePath) {
		boolean success = true;
		Charset charset = Charset.forName("UTF-8");
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), charset))) {
			String line = reader.readLine();
			String[] words;
			while(line != null) {
				words = line.split("\t");
				addTask(new SimmilarityTask(words[0], Integer.valueOf(words[1]),Integer.valueOf(words[2])), 1);
				line = reader.readLine();
			}
		} catch (IOException e) {
			success = false;
			System.out.println(e);						
		}
		return success;
	}

	@Override
	public void setup(EmbeddingRepository repo) {
		
	}

	@Override
	public void cleanup(EmbeddingRepository repo) {
		
	}

}