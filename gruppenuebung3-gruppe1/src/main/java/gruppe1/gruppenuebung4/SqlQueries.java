package gruppe1.gruppenuebung4;

public class SqlQueries {

	private SqlQueries() {

	}

	public static final String CREATE_EMBEDDINGS_TABLE = "CREATE TABLE public.embeddings(word character varying NOT NULL,vector cube NOT NULL,point point NOT NULL, year int NOT NULL,PRIMARY KEY (word,year))";
	public static final String CREATE_HASH_INDEX = "CREATE INDEX word_hash_index ON embeddings USING hash(word);";
	public static final String CREATE_BTREE_INDEX = "CREATE INDEX word_btree_index ON embeddings USING btree(word);";
	public static final String CREATE_GIST_INDEX = "CREATE INDEX vector_gist_index ON  embeddings USING gist (vector);";
	public static final String CREATE_QUADTREE_INDEX = "CREATE INDEX point_quadtree_index ON  embeddings USING spgist(point quad_point_ops);";
	
	public static final String CREATE_CUBE_EXTENSTION = "CREATE EXTENSION IF NOT EXISTS cube";
	public static final String DELETE_ALL_INDEXES = "DROP INDEX IF EXISTS word_hash_index; DROP INDEX IF EXISTS vector_gist_index;DROP INDEX IF EXISTS word_btree_index; DROP INDEX IF EXISTS point_quadtree_index;";
	
	public static final String CREATE_KNN_FUNCTION = "CREATE OR REPLACE FUNCTION getKNN( word_input varchar, k integer, year_input integer) \r\n" +
			"RETURNS TABLE(neighbor character varying, sim double precision) AS \r\n" + 
			"$$ DECLARE\r\n" + 
			"	search cube;\r\n" + 
			"BEGIN\r\n" +
			"	SELECT vector INTO search FROM embeddings WHERE embeddings.word=word_input AND embeddings.year = year_input;" +
			"	RETURN QUERY  SELECT embeddings.word as neighbor, (embeddings.vector <-> search) as sim FROM embeddings WHERE embeddings.year = year_input AND embeddings.word != word_input order by sim asc limit k;\r\n" + 
			"END;$$\r\n" + 
			"LANGUAGE PLPGSQL;";
	
	public static final String CREATE_KNN_POINT_FUNCTION = "CREATE OR REPLACE FUNCTION getKNN( word_input varchar, k integer, year_input integer) \r\n" +
			"RETURNS TABLE(neighbor character varying, sim double precision) AS \r\n" + 
			"$$ DECLARE\r\n" + 
			"	search point;\r\n" + 
			"BEGIN\r\n" +
			"	SELECT point INTO search FROM embeddings WHERE embeddings.word=word_input AND embeddings.year = year_input;" +
			"	RETURN QUERY  SELECT embeddings.word as neighbor, (embeddings.point <-> search) as sim FROM embeddings WHERE embeddings.year = year_input AND embeddings.word != word_input order by sim asc limit k;\r\n" + 
			"END;$$\r\n" + 
			"LANGUAGE PLPGSQL;";
	
	public static final String CREATE_NEIGHBORHOOD_CHANGE_FUNCTION = "CREATE OR REPLACE FUNCTION getNeighborhoodChange(word character varying, k integer, year1 integer, year2 integer)\n" + 
			"			RETURNS TABLE(neighbor character varying, exclusive_in integer) AS \n" + 
			"			$$ \n" + 
			"			BEGIN\n" + 
			"				DROP TABLE IF EXISTS neighbors1 ;\n" + 
			"				DROP TABLE IF EXISTS neighbors2 ;\n" + 
			"				DROP TABLE IF EXISTS neighbors_change ;\n" + 
			"				CREATE TEMPORARY TABLE neighbors1 (neighbor varchar, year int);\n" + 
			"				CREATE TEMPORARY TABLE neighbors2 (neighbor varchar, year int);\n" + 
			"				CREATE TEMPORARY TABLE neighbors_change (neighbor varchar, year int);\n" + 
			"				\n" + 
			"				INSERT INTO neighbors1 (SELECT getKNN.neighbor, year1 FROM getKNN(word, k, year1));\n" + 
			"				INSERT INTO neighbors2 (SELECT getKNN.neighbor, year2 FROM getKNN(word, k, year2));\n" + 
			"				\n" + 
			"				INSERT INTO neighbors_change (SELECT differences.neighbor, differences.year as exclusive_in FROM  \n" + 
			"											     (SELECT combined.neighbor, combined.year, count(*) FROM \n" + 
			"											         (SELECT * FROM neighbors1 UNION (SELECT * FROM neighbors2)) AS combined\n" + 
			"											     GROUP BY combined.neighbor, combined.year HAVING count(*) <= 1) AS differences\n" + 
			"												);														   \n" + 
			"				RETURN QUERY SELECT * FROM neighbors_change; \n" + 
			"	\n" + 
			"			END;$$ \n" + 
			"			LANGUAGE PLPGSQL;;";
	
	public static final String CREATE_SIM_FUNCTION = "CREATE OR REPLACE FUNCTION sim(word1 varchar, year1 integer, word2 varchar, year2 integer) \r\n" +
			"RETURNS double precision AS\r\n" + 
			"$$ DECLARE "
			+ "  simmilarity double precision;\r\n"
			+ " BEGIN\r\n" +
			"  SELECT (cube_ll_coord(e1.vector,1)*cube_ll_coord(e2.vector,1)) + (cube_ll_coord(e1.vector,2)*cube_ll_coord(e2.vector,2)) + (cube_ll_coord(e1.vector,3)*cube_ll_coord(e2.vector,3)) + (cube_ll_coord(e1.vector,4)*cube_ll_coord(e2.vector,4)) +(cube_ll_coord(e1.vector,5)*cube_ll_coord(e2.vector,5))  INTO simmilarity FROM embeddings e1, embeddings e2 WHERE e1.word = word1 AND e2.word = word2 AND e1.year = year1 AND e2.year = year2 ;\r\n"
			 + "	RETURN simmilarity; \r\n" + 
			" END;$$\r\n" + 
			"LANGUAGE PLPGSQL;";
}
