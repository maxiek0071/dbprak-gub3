package gruppe1.gruppenuebung4;

import java.sql.SQLException;

public class SimiliartyHashBenchmark extends SimmilarityBenchmark {

	public SimiliartyHashBenchmark(String name) {
		super(name);
	}

	@Override
	public void setup(EmbeddingRepository repo) {
		try {
			repo.createHashIndex();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup(EmbeddingRepository repo) {
		repo.deleteAllIndexes();
	}
}
