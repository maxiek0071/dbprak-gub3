package gruppe1.gruppenuebung3;

import java.sql.SQLException;

public class AnalogyGistBenchmark extends AnalogyBenchmark {
	public AnalogyGistBenchmark() {
		super("Gist");
		// TODO Auto-generated constructor stub
	}

	@Override
	public void setup(EmbeddingRepository repo) {
		try {
			repo.createGistIndex();
		} catch (SQLException e) {
		}
	}

	@Override
	public void cleanup(EmbeddingRepository repo) {
		repo.deleteAllIndexes();
	}
}
