package gruppe1.gruppenuebung3;

public class SqlQueries {

	private SqlQueries() {

	}

	public static final String EMBEDDINGS_TABLE = "CREATE TABLE public.embeddings(word_id int,vector cube NOT NULL, year int NOT NULL,"
			+ "  FOREIGN KEY (word_id) REFERENCES dict (id))";
	public static final String DICT_HASH_INDEX = "CREATE INDEX ON dict USING hash (word);";
	public static final String DICT_TABLE = "CREATE TABLE dict(id integer primary key, word character varying NOT NULL);";
	public static final String CUBE_EXTENSTION = "CREATE EXTENSION IF NOT EXISTS cube";
	public static final String DELETE_ALL_INDEXES = "CREATE OR REPLACE FUNCTION drop_all_indexes() RETURNS INTEGER AS $$\r\n" + 
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
}
