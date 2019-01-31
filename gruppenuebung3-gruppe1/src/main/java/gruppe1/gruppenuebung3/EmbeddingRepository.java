 package gruppe1.gruppenuebung3;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

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
			stmt.executeUpdate(SqlQueries.CREATE_EMBEDDINGS_TABLE);

			stmt.close();
			repo = new EmbeddingRepository(serverCon);
			repo.createFunctionsForNearestNeighbors();

		} catch (SQLException e) {
			repo = null;
			e.printStackTrace();
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
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public boolean importData(String path) throws SQLException, IOException {
		boolean success = false;
		try(BufferedReader in = new BufferedReader(new FileReader(new File(path+ "out-normalized.csv")));) {
			String insertStmt = "INSERT INTO embeddings (word,vector,point, year) VALUES (?,?::cube,?::point,?); ";
		
			try	(PreparedStatement st = con.prepareStatement(insertStmt)){
				String line;
				boolean skipFirst = true;
				while ((line = in.readLine() ) != null) {
					
					if (skipFirst) {
						skipFirst = false;
						continue;
					}
					String word = line.substring(0, line.indexOf(";"));
					st.setString(1, word);
					st.setObject(2,dimsToObject(line,5));
					st.setObject(3,dimsToObject(line,2));
					st.setInt(4, new Integer(line.split(";")[6]));
					st.addBatch();
					
				}
				st.executeBatch();
				success = true;
			}}
				return success;
		}


		private String dimsToObject( String line, int resultSize) {
			String allDims = line.substring(line.indexOf(";") + 1, line.lastIndexOf(";")).replace(";", ",");
			String[] dimsSplitted = allDims.split(",");
			String limitedDims = "";
			for (int i =0; i< resultSize;i++) {
				limitedDims += dimsSplitted[i] + ",";
			}
			limitedDims = "(" + limitedDims.substring(0,limitedDims.length()-1) + ")";
			return limitedDims;
		}
	
	public void createGistIndex() throws SQLException {
		Statement statement = con.createStatement();
		statement.execute(SqlQueries.CREATE_GIST_INDEX);
		statement.close();
	}
	
	public void createHashIndex() throws SQLException{
		Statement statement = con.createStatement();
		statement.execute(SqlQueries.CREATE_HASH_INDEX);
		statement.close();
		
	}
	public void createBTreeIndex() throws SQLException{
		Statement statement = con.createStatement();
		statement.execute(SqlQueries.CREATE_BTREE_INDEX);
		statement.close();
		
	}
	
	public void deleteAllIndexes() {
		try (Statement statement = con.createStatement()) {
			statement.execute(SqlQueries.DELETE_ALL_INDEXES);
			statement.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} 
	}
	
	public QueryResult<ArrayList<Neighbor>> getNeighborhoodChange(String w1,int year1, int year2) throws SQLException {
			
			PreparedStatement neighborStatement = con.prepareStatement("SELECT * FROM getNeighborhoodChange(?,?,?,?);");
			
			neighborStatement.setString(1, w1);
			neighborStatement.setInt(2, 10);
			neighborStatement.setInt(3, year1);
			neighborStatement.setInt(4, year2);
			
			long startTime = System.currentTimeMillis();
			ResultSet rs = neighborStatement.executeQuery();
			long runTime = System.currentTimeMillis() - startTime;
			
			ArrayList<Neighbor> neighbor = new  ArrayList<>();
			
			while (rs.next()) {
				neighbor.add(new Neighbor(rs.getString(1), rs.getInt(2)));
			}
			
			return new QueryResult<ArrayList<Neighbor>>(neighbor, runTime);
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
