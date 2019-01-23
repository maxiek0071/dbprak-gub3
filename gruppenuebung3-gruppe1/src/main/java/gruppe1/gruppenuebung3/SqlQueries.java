package gruppe1.gruppenuebung3;

public class SqlQueries {

	private SqlQueries() {

	}

	public static final String CREATE_EMBEDDINGS_TABLE = "CREATE TABLE public.embeddings(word_id int,vector cube NOT NULL, year int NOT NULL,"
			+ "  FOREIGN KEY (word_id) REFERENCES dict (id))";
	public static final String CREATE_DICT_HASH_INDEX = "CREATE INDEX ON dict USING hash (word);";
	public static final String CREATE_DICT_TABLE = "CREATE TABLE dict(id integer primary key, word character varying NOT NULL);";
	public static final String CREATE_CUBE_EXTENSTION = "CREATE EXTENSION IF NOT EXISTS cube";
	public static final String CREATE_DELETE_ALL_INDEXES_FUNCTION = "CREATE OR REPLACE FUNCTION drop_all_indexes() RETURNS INTEGER AS $$\r\n" + 
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
	
	public static final String CREATE_KNN_FUNCTION = "CREATE OR REPLACE FUNCTION getKNN( word_input varchar, k integer, year_input integer) \r\n" +
			"RETURNS TABLE(neighbor character varying, sim double precision) AS \r\n" + 
			"$$ DECLARE\r\n" + 
			"	search cube;\r\n" + 
			"BEGIN\r\n" +
			"	SELECT vector INTO search FROM embeddings, dict WHERE embeddings.word_id=id AND dict.word = word_input;" +
			"	RETURN QUERY  SELECT dict.word as neighbor, (embeddings.vector <-> search) as sim FROM embeddings, dict WHERE embeddings.word_id = dict.id AND embeddings.year = year_input order by sim asc limit k;\r\n" + 
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
			"  SELECT embeddings1.vector <-> embeddings2.vector INTO simmilarity FROM embeddings embeddings1, embeddings embeddings2, dict dict1, dict dict2 WHERE embeddings1.word_id = dict1.id AND embeddings2.word_id = dict2.id AND dict1.word = word1 AND dict2.word = word2 AND embeddings1.year = year1 AND embeddings2.year = year2 ;\r\n"
			 + "	RETURN simmilarity; \r\n" + 
			" END;$$\r\n" + 
			"LANGUAGE PLPGSQL;";
	
	
}
