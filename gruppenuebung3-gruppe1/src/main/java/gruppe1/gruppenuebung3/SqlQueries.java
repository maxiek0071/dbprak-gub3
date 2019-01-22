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
	
	public static final String CREATE_KNN_FUNCTION = "CREATE OR REPLACE FUNCTION getKNN( word varchar, k integer, year integer) \r\n" +
			"RETURNS TABLE(neighbor character varying, sim double precision) AS \r\n" + 
			"$$ DECLARE\r\n" + 
			"	search cube;\r\n" + 
			"BEGIN\r\n" +
			"	SELECT vector INTO search FROM embeddings, dict WHERE embeddings.word_id=id AND dict.word =word;" +
			"	RETURN QUERY  SELECT embeddings.word as neighbor, (embeddings.vector <-> search) as sim FROM embeddings WHERE embeddings.year = year order by sim asc limit k;\r\n" + 
			"END;$$\r\n" + 
			"LANGUAGE PLPGSQL;";
	
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
