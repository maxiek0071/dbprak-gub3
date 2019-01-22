package gruppe1.gruppenuebung3;

import java.sql.SQLException;

public class SimmilarityTask implements BenchmarkTask{
	private String w1; 
	private int year1;
	private int year2;
	
	public SimmilarityTask(String w1, int year1,int year2) {
		this.w1 = w1;
		this.year1 = year1;
		this.year2 = year2;
	}

	@Override
	public TaskResult run(EmbeddingRepository repo) throws SQLException {
		QueryResult<Double> result = repo.getCosSimilarity(w1, year1, w1, year2);
		result.getResult();
		return new TaskResult(result.getRunTime(), true);
	}

}