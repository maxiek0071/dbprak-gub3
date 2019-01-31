package gruppe1.gruppenuebung4;

import java.sql.SQLException;

public class NeighborQuadtreeBenchmark extends NeighborsBenchmark {

	public NeighborQuadtreeBenchmark(String name) {
		super(name);
	}
	
	@Override
	public void setup(EmbeddingRepository repo) {
		repo.setKnnColumnTo("point");
		try {
			repo.deleteAllIndexes();
			repo.createQuadtreeIndex();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup(EmbeddingRepository repo) {
		repo.deleteAllIndexes();
	}

}
