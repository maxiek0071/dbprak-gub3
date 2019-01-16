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

			// Create Table for Data
			stmt = serverCon.createStatement();
			stmt.executeUpdate("CREATE EXTENSION IF NOT EXISTS cube");
			stmt.executeUpdate("			CREATE TABLE dict(\r\n" + 
					"					id integer primary key ,\r\n" + 
					"					word character varying NOT NULL);");
			String createTable = "CREATE TABLE public.embeddings\r\n" + 
					"(\r\n" + 
					"  word_id int,\r\n" + 
					"  vector cube NOT NULL,\r\n" +
					"  year int NOT NULL,\r\n" +					
					"  FOREIGN KEY (word_id) REFERENCES dict (id)\r\n" + 
					")";
			stmt.executeUpdate(createTable);

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
		String analogySelect = "cube(ARRAY[";

		for (int i = 1; i < 6; i++) {
			analogySelect += "(cube_ll_coord(a2.vector, " + i + ")  - cube_ll_coord(a1.vector, " + i + ") + cube_ll_coord(b1.vector, " + i + ")),";
		}
		analogySelect = analogySelect.substring(0, analogySelect.length() - 1);
		analogySelect += "])";
		
		String function3 = "CREATE OR REPLACE FUNCTION getAnalogousWord(a1w varchar, a2w varchar, b1w varchar) \r\n" + 
				"RETURNS TABLE(word character varying, sim double precision) AS\r\n" + 
				"$$ DECLARE\r\n" + 
				"	contains integer;\r\n" + 
				"	distinctWords integer;\r\n" + 
				"	b2 cube;\r\n" + 
				"BEGIN\r\n" + 
				"	SELECT count(DISTINCT input.word) INTO distinctWords FROM (Values (a1w), (a2w), (b1w)) input (word);\r\n" + 
				"	SELECT count(embeddings.word) INTO contains FROM embeddings WHERE embeddings.word=a1w OR embeddings.word=a2w OR embeddings.word=b1w;\r\n" + 
				"	IF contains <  distinctWords THEN\r\n" + 
				"		RAISE 'missing % word(s)', distinctWords - contains; \r\n" + 
				"	ELSE\r\n" + 
				"		SELECT " + analogySelect + " INTO b2\r\n" + 
				"		FROM embeddings a1, embeddings a2, embeddings b1\r\n" + 
				"		WHERE a1.word = a1w AND a2.word = a2w AND b1.word =b1w;\r\n" +
				"	END IF;\r\n" + 
				"																			   \r\n" + 
				"																			   \r\n" + 
				"	RETURN QUERY  SELECT embeddings.word, (embeddings.vector <-> b2) as sim FROM embeddings order by sim asc limit 1;\r\n" + 
				"END;$$\r\n" + 
				"LANGUAGE PLPGSQL;";
		
		String deleteIndexes = "CREATE OR REPLACE FUNCTION drop_all_indexes() RETURNS INTEGER AS $$\r\n" + 
				"BEGIN\r\n" + 
				"   EXECUTE (\r\n" + 
				"   SELECT 'DROP INDEX ' || string_agg(indexrelid::regclass::text, ', ')\r\n" + 
				"   FROM   pg_index  i\r\n" + 
				"   LEFT   JOIN pg_depend d ON d.objid = i.indexrelid\r\n" + 
				"                          AND d.deptype = 'i'\r\n" + 
				"   WHERE  i.indrelid = 'embeddings'::regclass\r\n" + 
				"   AND    d.objid IS NULL\r\n" + 
				"   );\r\n" + 
				"RETURN 1;\r\n" + 
				"END\r\n" + 
				"$$ LANGUAGE plpgsql;";

		try (Statement statement = con.createStatement()){
			statement.execute(function3);
			statement.execute(deleteIndexes);
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
	
	
	
	
	public QueryResult<Boolean> containsWord(String word) throws SQLException {
		PreparedStatement stmt = con.prepareStatement("SELECT WORD FROM EMBEDDINGS WHERE word=?");
		stmt.setString(1, word);
		long startTime = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery();
		long runTime = System.currentTimeMillis() - startTime;
		boolean contains = rs.next();
		rs.close();
		stmt.close();
		return new QueryResult<Boolean>(new Boolean(contains), runTime);
	}


	public QueryResult<String> getAnalogousWord(String a1, String a2, String b1) throws SQLException {
		String result = null;
				String sql = "SELECT * FROM getAnalogousWord(?, ?, ?)";
				PreparedStatement preparedStatement2 = con.prepareStatement(sql);
				preparedStatement2.setString(1, a1);
				preparedStatement2.setString(2, a2);
				preparedStatement2.setString(3, b1);
				long start = System.currentTimeMillis();
			try {
				
				ResultSet rs = preparedStatement2.executeQuery();
				if(rs.next()) {
					result = rs.getString(1);
				}
				rs.close();
			} catch (SQLException e) {
				throw e;
			} finally {
				
				preparedStatement2.close();
			}
			
			long runtime = System.currentTimeMillis() - start;
			
		return new QueryResult<String>(result, runtime);
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
	public QueryResult<List<WordResult>> getKNearestNeighbors(int k, String word) throws SQLException {

		PreparedStatement stmt = con.prepareStatement("SELECT * FROM getknearestneighbors(?,?);");
		stmt.setString(1, word);
		stmt.setInt(2, k);

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
	
	
	public void disconnect() {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {

			}
		}
	}
}
