package gruppe1.gruppenuebung4;

import java.sql.SQLException;

public class SimiliartyBTreeBenchmark extends SimmilarityBenchmark {

	public SimiliartyBTreeBenchmark(String name) {
		super(name);
	}

	@Override
	public void setup(EmbeddingRepository repo) {
		try {
			repo.createBTreeIndex();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup(EmbeddingRepository repo) {
		repo.deleteAllIndexes();
	}
}
