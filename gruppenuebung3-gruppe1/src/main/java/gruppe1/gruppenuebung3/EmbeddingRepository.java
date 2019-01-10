 package gruppe1.gruppenuebung3;

import java.io.BufferedReader;
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
	private static int DIMENSION = 5;
	private Connection con;
	private PreparedStatement simStatement;

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
			String createTable = "CREATE TABLE EMBEDDINGS (WORD VARCHAR not NULL,vector cube, ";
			
			createTable = createTable.concat(" LENGTH double precision, ");
			createTable = createTable.concat(" PRIMARY KEY (WORD)); ");
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
		String returnStatement = "";
		String analogySelect = "cube(ARRAY[";
		String lengthAttribute = "SQRT(";

		for (int i = 1; i < 101; i++) {
			returnStatement += "(cube_ll_coord(w1.vector," + i + ") * cube_ll_coord(w2.vector," + i + "))+";
			analogySelect += "(cube_ll_coord(a2.vector, " + i + ")  - cube_ll_coord(a1.vector, " + i + ") + cube_ll_coord(b1.vector, " + i + ")),";
			lengthAttribute += "pow(cube_ll_coord(a," + i + "), 2.0) + ";
		}
		returnStatement = returnStatement.substring(0, returnStatement.length() - 1);
		
		analogySelect = analogySelect.substring(0, analogySelect.length() - 1);
		analogySelect += "])";
		
		lengthAttribute = lengthAttribute.substring(0, lengthAttribute.length() - 2);
		lengthAttribute += ")";
		
		String cubeLengthFunction = "CREATE OR REPLACE FUNCTION length(a cube) \r\n" + 
				"RETURNS double precision AS\r\n" + 
				"$$ DECLARE\r\n" + 
				"	result double precision;\r\n" + 
				"BEGIN\n" +
				"SELECT " + lengthAttribute + " INTO result;\r\n" +
				"RETURN  result;" + 
				"END;$$\r\n" + 
				"LANGUAGE PLPGSQL;";
		
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
			statement.execute(cubeLengthFunction);
			statement.execute(deleteIndexes);
			simStatement = con.prepareStatement("SELECT (" + returnStatement + ")/(length(w1.vector)*length(w2.vector)) FROM embeddings w1, embeddings w2 WHERE w1.word=? AND w2.word=?");
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public boolean importData(BufferedReader in) throws SQLException, IOException {
		boolean success = false;
		String insertStmt = "INSERT INTO embeddings (word,vector) VALUES (?,?::cube); ";
		
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
			st.setObject(2,dimsToCube(line));
			st.addBatch();
			
			 ;
		}
		st.executeBatch();
		success = true;
	}
		return success;
	}

	private String dimsToCube( String line) {
		String allDims = line.substring(line.indexOf(";") + 1, line.lastIndexOf(';')).replace(";", ",");
		String[] dimsSplitted = allDims.split(",");
		String limitedDims = "";
		for (int i =0; i< 100;i++) {
			limitedDims += dimsSplitted[i] + ",";
		}
		limitedDims = "(" + limitedDims.substring(0,limitedDims.length()-1) + ")";
		return limitedDims;
	}
	
	public void indexingWordColumn() {
		try (Statement statement = con.createStatement()) {
			statement.execute("DROP INDEX IF EXISTS word_indexing;\r\n"
					+ "CREATE INDEX word_indexing ON embeddings USING btree(word);");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void deleteAllIndexes() {
		try (Statement statement = con.createStatement()) {
			statement.execute("SELECT drop_all_indexes()");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public QueryResult<String> createMaterializedSimView() {
		// TODO replace returnStatement with a statement related to the new database
		// structure
		StringBuilder returnStatement = new StringBuilder();
		for (int i = 1; i < 301; i++) {
			returnStatement.append("cube_ll_coord(e1.vector, " + i + ") * cube_ll_coord(e2.vector, " + i + ")");
			if (i < 300) {
				returnStatement.append("+");
			}
		}
		try (Statement statement = con.createStatement()) {
			long startTime = System.currentTimeMillis();
			statement.execute("CREATE MATERIALIZED VIEW sim_table (word_1, word_2, cos_sim) AS\r\n"
					+ "(SELECT e1.word, e2.word, " + returnStatement.toString() + "\r\n"
					+ "FROM embeddings e1, embeddings e2\r\n" + "WHERE e1.word <> e2.word\r\n" //
					+ "AND e1.word < e2.word" // (avoids duplicates; remove this for a symmetric view)
					+ ")");
			long endTime = System.currentTimeMillis();
			// get size of view
			ResultSet resultSet = statement.executeQuery("SELECT pg_size_pretty(pg_table_size(oid))\r\n"
					+ "FROM   pg_class\r\n" + "WHERE  relname = 'sim_table'");
			String result = "0";
			if (resultSet.next()) {
				result = resultSet.getString(1);
			}
			return new QueryResult<String>(result, endTime - startTime);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
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

	public QueryResult<Double> getCosSimilarity(String w1, String w2) throws SQLException {
		double simmilarity = -1;

		
		simStatement.setString(1, w1);
		simStatement.setString(2, w2);

		long startTime = System.currentTimeMillis();
		ResultSet rs = simStatement.executeQuery();
		long runTime = System.currentTimeMillis() - startTime;

		if (rs.next()) {
			simmilarity = rs.getDouble(1);
		}
		rs.close();
		
		return new QueryResult<Double>(new Double(simmilarity), runTime);
	}
	
	public void createGistIndex() throws SQLException {
		Statement statement = con.createStatement();
		statement.execute("DROP INDEX  IF EXISTS vector_gist_index;\n" + 
				"CREATE INDEX vector_gist_index ON  embeddings USING gist (vector);");
	}
	
	
	public void disconnect() {
		if (con != null) {
			try {
				simStatement.close();
				con.close();
			} catch (SQLException e) {

			}
		}
	}
}
