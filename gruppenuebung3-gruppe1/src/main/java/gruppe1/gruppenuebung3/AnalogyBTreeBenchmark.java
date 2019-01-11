package gruppe1.gruppenuebung3;

import java.sql.SQLException;

public class AnalogyBTreeBenchmark extends AnalogyBenchmark {
	
	
	public AnalogyBTreeBenchmark() {
		super("BTree");
	}

	@Override
	public void setup(EmbeddingRepository repo) {
		try {
			repo.createBTreeIndex();
		} catch (SQLException e) {
		}
	}

	@Override
	public void cleanup(EmbeddingRepository repo) {
		repo.deleteAllIndexes();
	}
}
