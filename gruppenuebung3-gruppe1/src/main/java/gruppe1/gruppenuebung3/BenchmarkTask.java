package gruppe1.gruppenuebung3;

import java.sql.SQLException;

public interface BenchmarkTask {
	
	public TaskResult run(EmbeddingRepository repo) throws SQLException;

}
