 package gruppe1.gruppenuebung3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class EmbeddingRepository {
		private Connection con;

	private EmbeddingRepository(Connection con) {
		this.con = con;

	}

	/**
	 * Creates and initializes a Repository for Embeddings Database and Table are
	 * created.
	 * 
	 * @param host
	 * @param port
	 * @param user
	 * @param password
	 * @return The Repository. Null if Connection to DB fails.
	 */
	public static EmbeddingRepository createRepository(String host, String port, String user, String password) {
		String url = "jdbc:postgresql://" + host + ":" + port + "/";
		Connection serverCon = null;
		EmbeddingRepository repo = null;

		try {
			serverCon = DriverManager.getConnection(url, user, password);
			Statement stmt = serverCon.createStatement();
			stmt.executeUpdate("DROP DATABASE IF EXISTS nlp");
			stmt.executeUpdate("CREATE DATABASE nlp");
			stmt.close();
			serverCon.close();

			serverCon = DriverManager.getConnection(url + "nlp", user, password);

//			// Create Table for Data
			stmt = serverCon.createStatement();
			stmt.executeUpdate(SqlQueries.CREATE_CUBE_EXTENSTION);
			stmt.executeUpdate(SqlQueries.CREATE_DICT_TABLE);
			stmt.executeUpdate(SqlQueries.CREATE_DICT_HASH_INDEX);
			stmt.executeUpdate(SqlQueries.CREATE_EMBEDDINGS_TABLE);

			stmt.close();
			repo = new EmbeddingRepository(serverCon);
			repo.createFunctionsForNearestNeighbors();

		} catch (SQLException e) {
			repo = null;
			if (serverCon != null) {
				try {
					serverCon.close();
				} catch (SQLException f) {
					f.printStackTrace();
				}

			}
		}
		return repo;
	}

	private void createFunctionsForNearestNeighbors() {
		try (Statement statement = con.createStatement()){
			statement.execute(SqlQueries.CREATE_KNN_FUNCTION);
			statement.execute(SqlQueries.CREATE_SIM_FUNCTION);
			statement.execute(SqlQueries.CREATE_NEIGHBORHOOD_CHANGE_FUNCTION);
			statement.execute(SqlQueries.CREATE_DELETE_ALL_INDEXES_FUNCTION);
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

public boolean importData(String path) throws SQLException, IOException {
	
	return importDictionary(path) && importEmbedding(path);
	}


private boolean importEmbedding(String path) throws SQLException, IOException {
	boolean success = false;
	try(BufferedReader in = new BufferedReader(new FileReader(new File(path+ "out-normalized.csv")));) {
		String insertStmt = "INSERT INTO embeddings (word_id,vector,year) VALUES (?,?::cube,?); ";
	
		try	(PreparedStatement st = con.prepareStatement(insertStmt)){
			String line;
			boolean skipFirst = true;
			while ((line = in.readLine() ) != null) {
				
				if (skipFirst) {
					skipFirst = false;
					continue;
				}
				String wordIndex = line.substring(0, line.indexOf(";"));
				st.setInt(1, new Integer(wordIndex));
				st.setObject(2,dimsToCube(line));
				st.setInt(3, new Integer(line.split(";")[6]));
				st.addBatch();
				
			}
			st.executeBatch();
			success = true;
		}}
			return success;
	}




private boolean importDictionary(String path) throws IOException, SQLException, FileNotFoundException {
	boolean success = false;
		try(BufferedReader in = new BufferedReader(new FileReader(new File(path+ "dict.csv")));) {
		
		String insertStmt = "INSERT INTO dict (id,word) VALUES (?,?); ";
		
	try	(PreparedStatement st = con.prepareStatement(insertStmt)){
			
		String line;
		while ((line = in.readLine() ) != null) {
			st.setInt(1, new Integer(line.split(";")[0]));
			st.setObject(2,line.split(";")[1]);
			st.addBatch();
		}
		st.executeBatch();
		success = true;
	}
		}
		return success;
}

	private String dimsToCube( String line) {
		String allDims = line.substring(line.indexOf(";") + 1, line.lastIndexOf(";")).replace(";", ",");
		String[] dimsSplitted = allDims.split(",");
		String limitedDims = "";
		for (int i =0; i< 5;i++) {
			limitedDims += dimsSplitted[i] + ",";
		}
		limitedDims = "(" + limitedDims.substring(0,limitedDims.length()-1) + ")";
		return limitedDims;
	}
	
	
	
	
	public QueryResult<Boolean> containsWord(String word, int year) throws SQLException {
		PreparedStatement stmt = con.prepareStatement("SELECT WORD FROM EMBEDDINGS, dict WHERE word_id=id AND word=? AND year=?");
		stmt.setString(1, word);
		long startTime = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery();
		long runTime = System.currentTimeMillis() - startTime;
		boolean contains = rs.next();
		rs.close();
		stmt.close();
		return new QueryResult<Boolean>(new Boolean(contains), runTime);
	}


	/**
	 * This method returns the k nearest neighbors of a given word using the cos
	 * similarity.
	 * 
	 * @param k
	 * @param word
	 * @return
	 * @throws SQLException
	 */
	public QueryResult<List<WordResult>> getKNearestNeighbors(int k, String word, int year) throws SQLException {

		PreparedStatement stmt = con.prepareStatement("SELECT * FROM getKNN(?,?,?);");
		stmt.setString(1, word);
		stmt.setInt(2, k);
		stmt.setInt(3, year);

		long startTime = System.currentTimeMillis();
		ResultSet result = stmt.executeQuery();
		long runTime = System.currentTimeMillis() - startTime;

		List<WordResult> results = new ArrayList<WordResult>();

		while (result.next()) {
			results.add(new WordResult(result.getString("word"), result.getDouble("sim")));
		}
		result.close();
		stmt.close();		
		
		return new QueryResult<List<WordResult>>(results, runTime);
	}
	
	

	
	public void createGistIndex() throws SQLException {
		deleteAllIndexes();
		Statement statement = con.createStatement();
		statement.execute("CREATE INDEX vector_gist_index ON  embeddings USING gist (vector);");
		statement.close();
	}
	
	public void createBTreeIndex() throws SQLException{
		deleteAllIndexes();
		Statement statement = con.createStatement();
		statement.execute("CREATE INDEX vector_btree_index ON embeddings USING btree(vector);");
		statement.close();
		
	}
	
	public void deleteAllIndexes() {
		try (Statement statement = con.createStatement()) {
			statement.execute("DROP INDEX IF EXISTS vector_btree_index; DROP INDEX IF EXISTS vector_gist_index;");
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	public QueryResult<Double> getCosSimilarity(String w1,int year1, String w2, int year2) throws SQLException {
		double simmilarity = -1;

		PreparedStatement simStatement = con.prepareStatement("SELECT * FROM sim(?,?,?,?);");
		
		simStatement.setString(1, w1);
		simStatement.setInt(2, year1);
		simStatement.setString(3, w2);
		simStatement.setInt(4, year2);

		long startTime = System.currentTimeMillis();
		ResultSet rs = simStatement.executeQuery();
		long runTime = System.currentTimeMillis() - startTime;

		if (rs.next()) {
			simmilarity = rs.getDouble(1);
			System.out.println("fin");
		}
		rs.close();
		
		return new QueryResult<Double>(new Double(simmilarity), runTime);
	}
	
	
	
	public void disconnect() {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {

			}
		}
	}
}
