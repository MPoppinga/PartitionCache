-- PartitionCache PostgreSQL Cache Handlers - Global Functions
-- This file contains functions that are globally needed by all PostgreSQL cache handlers
-- These functions handle table naming, metadata management, and partition bootstrapping

-- Function to get the target cache table name for a partition
CREATE OR REPLACE FUNCTION partitioncache_get_cache_table_name(p_table_prefix TEXT, p_partition_key TEXT)
RETURNS TEXT AS $$
BEGIN
    IF p_table_prefix = '' OR p_table_prefix IS NULL THEN
        RETURN 'cache_' || p_partition_key;
    ELSE
        RETURN p_table_prefix || '_cache_' || p_partition_key;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to get the metadata table name
CREATE OR REPLACE FUNCTION partitioncache_get_metadata_table_name(p_table_prefix TEXT)
RETURNS TEXT AS $$
BEGIN
    IF p_table_prefix = '' OR p_table_prefix IS NULL THEN
        RETURN 'partition_metadata';
    ELSE
        RETURN p_table_prefix || '_partition_metadata';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to get the queries table name
CREATE OR REPLACE FUNCTION partitioncache_get_queries_table_name(p_table_prefix TEXT)
RETURNS TEXT AS $$
BEGIN
    IF p_table_prefix = '' OR p_table_prefix IS NULL THEN
        RETURN 'queries';
    ELSE
        RETURN p_table_prefix || '_queries';
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function to ensure metadata and queries tables exist with correct naming
CREATE OR REPLACE FUNCTION partitioncache_ensure_metadata_tables(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_metadata_table TEXT;
    v_queries_table TEXT;
    v_cache_backend TEXT;
BEGIN
    v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);
    v_queries_table := partitioncache_get_queries_table_name(p_table_prefix);
    
    -- Determine cache backend type from table prefix
    IF p_table_prefix LIKE '%_roaringbit%' THEN
        v_cache_backend := 'roaringbit';
    ELSIF p_table_prefix LIKE '%_bit%' THEN
        v_cache_backend := 'bit';
    ELSE
        v_cache_backend := 'array';
    END IF;
    
    -- Create metadata table
    IF v_cache_backend = 'bit' THEN
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                partition_key TEXT PRIMARY KEY,
                datatype TEXT NOT NULL CHECK (datatype = ''integer''),
                bitsize INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT now()
            )', v_metadata_table);
    ELSIF v_cache_backend = 'roaringbit' THEN
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                partition_key TEXT PRIMARY KEY,
                datatype TEXT NOT NULL CHECK (datatype = ''integer''),
                created_at TIMESTAMP DEFAULT now()
            )', v_metadata_table);
    ELSE
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                partition_key TEXT PRIMARY KEY,
                datatype TEXT NOT NULL CHECK (datatype IN (''integer'', ''float'', ''text'', ''timestamp'')),
                created_at TIMESTAMP DEFAULT now()
            )', v_metadata_table);
    END IF;
    
    -- Create queries table with status
    EXECUTE format('
        CREATE TABLE IF NOT EXISTS %I (
            query_hash TEXT NOT NULL,
            partition_key TEXT NOT NULL,
            query TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT ''ok'' CHECK (status IN (''ok'', ''timeout'', ''failed'', ''limit'')),
            last_seen TIMESTAMP NOT NULL DEFAULT now(),
            PRIMARY KEY (query_hash, partition_key)
        )', v_queries_table);
END;
$$ LANGUAGE plpgsql;

-- Function to bootstrap a partition (create metadata entry and cache table)
CREATE OR REPLACE FUNCTION partitioncache_bootstrap_partition(
    p_table_prefix TEXT, 
    p_partition_key TEXT, 
    p_datatype TEXT,
    p_cache_backend TEXT,
    p_bitsize INTEGER DEFAULT NULL
)
RETURNS BOOLEAN AS $$
DECLARE
    v_metadata_table TEXT;
    v_cache_table TEXT;
    v_partition_exists BOOLEAN;
    v_index_name TEXT;
BEGIN
    v_metadata_table := partitioncache_get_metadata_table_name(p_table_prefix);
    v_cache_table := partitioncache_get_cache_table_name(p_table_prefix, p_partition_key);
    
    -- Check if partition already exists
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I WHERE partition_key = %L)', v_metadata_table, p_partition_key)
    INTO v_partition_exists;
    
    IF v_partition_exists THEN
        RETURN true; -- Already exists, nothing to do
    END IF;
    
    -- Create metadata entry based on cache backend
    IF p_cache_backend = 'bit' THEN
        -- For bit cache, include bitsize
        EXECUTE format(
            'INSERT INTO %I (partition_key, datatype, bitsize) VALUES (%L, %L, %L) ON CONFLICT(partition_key) DO NOTHING',
            v_metadata_table, p_partition_key, p_datatype, p_bitsize
        );
        
        -- Create bit cache table with fixed BIT size
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                query_hash TEXT PRIMARY KEY,
                partition_keys BIT(%s),
                partition_keys_count INTEGER GENERATED ALWAYS AS (length(replace(partition_keys::text, ''0'', ''''))) STORED
            )', v_cache_table, p_bitsize);
        
        -- Create bitsize trigger for this table prefix
        PERFORM partitioncache_create_bitsize_trigger(p_table_prefix);
    ELSIF p_cache_backend = 'roaringbit' THEN
        -- For roaring bit cache
        EXECUTE format(
            'INSERT INTO %I (partition_key, datatype) VALUES (%L, %L) ON CONFLICT(partition_key) DO NOTHING',
            v_metadata_table, p_partition_key, p_datatype
        );
        
        -- Create roaring bitmap cache table (enable extension first)
        EXECUTE 'CREATE EXTENSION IF NOT EXISTS roaringbitmap';
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                query_hash TEXT PRIMARY KEY,
                partition_keys roaringbitmap,
                partition_keys_count INTEGER GENERATED ALWAYS AS (rb_cardinality(partition_keys)) STORED
            )', v_cache_table);
    ELSE -- 'array'
        -- For array cache
        EXECUTE format(
            'INSERT INTO %I (partition_key, datatype) VALUES (%L, %L) ON CONFLICT(partition_key) DO NOTHING',
            v_metadata_table, p_partition_key, p_datatype
        );
        
        -- Create array cache table with proper datatype
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I (
                query_hash TEXT PRIMARY KEY,
                partition_keys %s[],
                partition_keys_count integer GENERATED ALWAYS AS (
                    CASE
                        WHEN partition_keys IS NULL THEN NULL
                        ELSE cardinality(partition_keys)
                    END
                ) STORED
            )', v_cache_table, 
            CASE p_datatype 
                WHEN 'integer' THEN 'INTEGER'
                WHEN 'float' THEN 'NUMERIC'
                WHEN 'text' THEN 'TEXT'
                WHEN 'timestamp' THEN 'TIMESTAMP'
                ELSE 'TEXT'
            END);
        
        -- Create optimized indexes for array cache
        v_index_name := 'idx_' || replace(v_cache_table, '.', '_') || '_partition_keys';
        IF p_datatype = 'integer' THEN
            -- Try intarray-specific GIN index for integers first
            BEGIN
                EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I USING GIN (partition_keys gin__int_ops)', 
                              v_index_name, v_cache_table);
            EXCEPTION WHEN OTHERS THEN
                -- Fallback to standard GIN index if intarray extension not available
                EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I USING GIN (partition_keys)', 
                              v_index_name, v_cache_table);
            END;
        ELSE
            -- Use standard GIN index for other types
            EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I USING GIN (partition_keys)', 
                          v_index_name, v_cache_table);
        END IF;
    END IF;
    
    RETURN true;
END;
$$ LANGUAGE plpgsql;

-- Function to create bitsize trigger for PostgreSQL bit cache handler
CREATE OR REPLACE FUNCTION partitioncache_create_bitsize_trigger(p_table_prefix TEXT)
RETURNS VOID AS $$
DECLARE
    v_trigger_function_name TEXT;
    v_trigger_name TEXT;
    v_metadata_table TEXT;
BEGIN
    v_trigger_function_name := p_table_prefix || '_bitsize_trigger_func';
    v_trigger_name := p_table_prefix || '_bitsize_trigger';
    v_metadata_table := p_table_prefix || '_partition_metadata';
    
    -- Create trigger function
    EXECUTE format('
        CREATE OR REPLACE FUNCTION %I()
        RETURNS TRIGGER AS $trigger$
        DECLARE
            v_cache_table TEXT;
        BEGIN
            -- Only handle UPDATE operations where bitsize changes
            IF TG_OP = ''UPDATE'' AND OLD.bitsize != NEW.bitsize THEN
                v_cache_table := ''%s_cache_'' || NEW.partition_key;
                
                -- Check if cache table exists
                IF EXISTS (SELECT 1 FROM information_schema.tables
                          WHERE table_name = v_cache_table AND table_schema = ''public'') THEN
                    -- ALTER the cache table to use new bitsize
                    EXECUTE format(''ALTER TABLE %%I ALTER COLUMN partition_keys TYPE BIT(%%s)'',
                                 v_cache_table, NEW.bitsize);
                END IF;
            END IF;
            
            RETURN NEW;
        END;
        $trigger$ LANGUAGE plpgsql;
    ', v_trigger_function_name, p_table_prefix);
    
    -- Drop existing trigger if it exists
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', v_trigger_name, v_metadata_table);
    
    -- Create trigger
    EXECUTE format('
        CREATE TRIGGER %I
            AFTER UPDATE ON %I
            FOR EACH ROW
            EXECUTE FUNCTION %I()
    ', v_trigger_name, v_metadata_table, v_trigger_function_name);
    
END;
$$ LANGUAGE plpgsql;

-- Function to setup array-specific extensions and aggregates
CREATE OR REPLACE FUNCTION partitioncache_setup_array_extensions()
RETURNS BOOLEAN AS $$
DECLARE
    v_intarray_available BOOLEAN := false;
    v_aggregate_created BOOLEAN := false;
BEGIN
    -- Try to enable intarray extension for better integer performance
    BEGIN
        EXECUTE 'CREATE EXTENSION IF NOT EXISTS intarray';
        v_intarray_available := true;
    EXCEPTION WHEN OTHERS THEN
        -- Extension not available, continue without it
        v_intarray_available := false;
    END;
    
    -- Create custom aggregate for array intersection
    BEGIN
        -- Create the intersection function
        EXECUTE '
            CREATE OR REPLACE FUNCTION _array_intersect(anyarray, anyarray)
            RETURNS anyarray AS $func$
                SELECT CASE
                    WHEN $1 IS NULL THEN $2
                    WHEN $2 IS NULL THEN $1
                    ELSE ARRAY(SELECT unnest($1) INTERSECT SELECT unnest($2) ORDER BY 1)
                END;
            $func$ LANGUAGE sql IMMUTABLE;
        ';
        
        -- Drop and recreate the aggregate
        EXECUTE 'DROP AGGREGATE IF EXISTS array_intersect_agg(anyarray)';
        EXECUTE '
            CREATE AGGREGATE array_intersect_agg(anyarray) (
                SFUNC = _array_intersect,
                STYPE = anyarray
            );
        ';
        v_aggregate_created := true;
    EXCEPTION WHEN OTHERS THEN
        v_aggregate_created := false;
    END;
    
    RETURN v_intarray_available AND v_aggregate_created;
END;
$$ LANGUAGE plpgsql;