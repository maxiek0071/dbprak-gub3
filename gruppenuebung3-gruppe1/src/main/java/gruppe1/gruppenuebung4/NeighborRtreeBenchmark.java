package gruppe1.gruppenuebung4;

import java.sql.SQLException;

public class NeighborRtreeBenchmark extends NeighborsBenchmark {

	public NeighborRtreeBenchmark(String name) {
		super(name);
	}
	
	@Override
	public void setup(EmbeddingRepository repo) {
		repo.setKnnColumnTo("vector");
		try {
			repo.deleteAllIndexes();
			repo.createGistIndex();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup(EmbeddingRepository repo) {
		repo.deleteAllIndexes();
	}

}
