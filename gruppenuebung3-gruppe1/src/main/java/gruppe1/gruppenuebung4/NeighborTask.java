package gruppe1.gruppenuebung4;

import java.sql.SQLException;
import java.util.ArrayList;

public class NeighborTask implements BenchmarkTask {
	private String w1; 
	private int year1;
	private int year2;
	
	public NeighborTask(String w1, int year1,int year2) {
		this.w1 = w1;
		this.year1 = year1;
		this.year2 = year2;
	}

	@Override
	public TaskResult run(EmbeddingRepository repo) throws SQLException {
		QueryResult<ArrayList<Neighbor>> result = repo.getNeighborhoodChange(w1, year1, year2);
		result.getResult();
		return new TaskResult(result.getRunTime(), true);
	}
}
