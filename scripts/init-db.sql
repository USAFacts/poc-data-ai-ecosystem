-- Pipeline logging table — created on first docker compose up.
-- The SQLAlchemy model (db/models.py PipelineLogModel) is the source of truth;
-- this script bootstraps the table so logs can be written before any Python
-- migration runs.

CREATE TABLE IF NOT EXISTS pipeline_logs (
    id            SERIAL PRIMARY KEY,
    timestamp     TIMESTAMPTZ NOT NULL DEFAULT now(),
    level         VARCHAR(10)  NOT NULL,
    logger_name   VARCHAR(255) NOT NULL,
    message       TEXT         NOT NULL,
    run_id        VARCHAR(255),
    workflow      VARCHAR(255),
    step          VARCHAR(255),
    asset         VARCHAR(255),
    extra         JSONB
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS ix_pipeline_logs_timestamp ON pipeline_logs (timestamp);
CREATE INDEX IF NOT EXISTS ix_pipeline_logs_level     ON pipeline_logs (level);
CREATE INDEX IF NOT EXISTS ix_pipeline_logs_run_id    ON pipeline_logs (run_id);
CREATE INDEX IF NOT EXISTS ix_pipeline_logs_workflow  ON pipeline_logs (workflow);

-- Add run_id column if upgrading from an older schema
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'pipeline_logs' AND column_name = 'run_id'
    ) THEN
        ALTER TABLE pipeline_logs ADD COLUMN run_id VARCHAR(255);
        CREATE INDEX IF NOT EXISTS ix_pipeline_logs_run_id ON pipeline_logs (run_id);
    END IF;
END $$;

-- ============================================================
-- Census Bureau Data API tables
-- Ported from: github.com/uscensusbureau/us-census-bureau-data-api-mcp
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Summary levels: geographic hierarchy definitions (State, County, Tract, etc.)
CREATE TABLE IF NOT EXISTS census_summary_levels (
    id              SERIAL PRIMARY KEY,
    code            VARCHAR(3) NOT NULL UNIQUE,
    name            VARCHAR(255) NOT NULL UNIQUE,
    description     TEXT,
    get_variable    VARCHAR(100),
    query_name      VARCHAR(100),
    on_spine        BOOLEAN DEFAULT FALSE,
    hierarchy_level INTEGER DEFAULT 0,
    parent_summary_level VARCHAR(3) REFERENCES census_summary_levels(code)
);
CREATE INDEX IF NOT EXISTS ix_census_sl_name_trgm ON census_summary_levels USING gin (name gin_trgm_ops);

-- Geographies: FIPS codes, names, coordinates, pre-computed Census API params
CREATE TABLE IF NOT EXISTS census_geographies (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(500) NOT NULL,
    full_name       VARCHAR(1000),
    state_code      CHAR(2),
    county_code     CHAR(3),
    fips_code       VARCHAR(20),
    census_geoid    VARCHAR(40),
    ucgid_code      VARCHAR(60) UNIQUE,
    summary_level_code VARCHAR(3) REFERENCES census_summary_levels(code),
    for_param       VARCHAR(255) NOT NULL,
    in_param        VARCHAR(255),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    population      BIGINT,
    land_area_sqkm  DOUBLE PRECISION,
    region_code     VARCHAR(5),
    division_code   VARCHAR(5),
    place_code      VARCHAR(10),
    year            INTEGER NOT NULL DEFAULT 2023,
    UNIQUE (fips_code, year)
);
CREATE INDEX IF NOT EXISTS ix_census_geo_name_trgm ON census_geographies USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS ix_census_geo_full_name_trgm ON census_geographies USING gin (full_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS ix_census_geo_summary_level ON census_geographies (summary_level_code);
CREATE INDEX IF NOT EXISTS ix_census_geo_state ON census_geographies (state_code);

-- Programs: top-level survey programs (e.g. "American Community Survey")
CREATE TABLE IF NOT EXISTS census_programs (
    id              SERIAL PRIMARY KEY,
    label           VARCHAR(500) NOT NULL,
    description     TEXT,
    acronym         VARCHAR(50) UNIQUE
);

-- Components: survey components (e.g. "ACS 1-Year Detailed Tables")
CREATE TABLE IF NOT EXISTS census_components (
    id              SERIAL PRIMARY KEY,
    label           VARCHAR(500) NOT NULL,
    component_id    VARCHAR(100) UNIQUE,
    api_endpoint    VARCHAR(255) UNIQUE,
    description     TEXT,
    program_id      INTEGER REFERENCES census_programs(id)
);

-- Datasets: Census datasets by year
CREATE TABLE IF NOT EXISTS census_datasets (
    id              SERIAL PRIMARY KEY,
    dataset_id      VARCHAR(255) NOT NULL UNIQUE,
    name            VARCHAR(500),
    api_endpoint    VARCHAR(255),
    description     TEXT,
    type            VARCHAR(50) DEFAULT 'aggregate',
    year            INTEGER,
    component_id    INTEGER REFERENCES census_components(id)
);

-- Data tables: Census table catalog (e.g. B01001, S0101)
CREATE TABLE IF NOT EXISTS census_data_tables (
    id              SERIAL PRIMARY KEY,
    data_table_id   VARCHAR(40) NOT NULL UNIQUE,
    label           TEXT
);
CREATE INDEX IF NOT EXISTS ix_census_dt_label_trgm ON census_data_tables USING gin (label gin_trgm_ops);

-- Junction: data tables <-> datasets
CREATE TABLE IF NOT EXISTS census_data_table_datasets (
    id              SERIAL PRIMARY KEY,
    dataset_id      INTEGER NOT NULL REFERENCES census_datasets(id),
    data_table_id   INTEGER NOT NULL REFERENCES census_data_tables(id),
    label           TEXT,
    UNIQUE (dataset_id, data_table_id)
);
CREATE INDEX IF NOT EXISTS ix_census_dtd_label_trgm ON census_data_table_datasets USING gin (label gin_trgm_ops);

-- Cache for Census API responses
CREATE TABLE IF NOT EXISTS census_data_cache (
    id              SERIAL PRIMARY KEY,
    request_hash    VARCHAR(64) NOT NULL UNIQUE,
    dataset_code    VARCHAR(255),
    year            INTEGER,
    variables       TEXT[],
    geography_spec  JSONB,
    response_data   JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ============================================================
-- Census search functions (trigram-based fuzzy matching)
-- ============================================================

CREATE OR REPLACE FUNCTION search_census_geographies(
    search_term TEXT,
    max_results INTEGER DEFAULT 10
)
RETURNS TABLE (
    id              INTEGER,
    name            VARCHAR(500),
    summary_level_name VARCHAR(255),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    for_param       VARCHAR(255),
    in_param        VARCHAR(255),
    weighted_score  REAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        g.id,
        g.name,
        sl.name AS summary_level_name,
        g.latitude,
        g.longitude,
        g.for_param,
        g.in_param,
        (SIMILARITY(g.name, search_term) + (1.0 - COALESCE(sl.hierarchy_level, 0)::real / 100.0))::real AS weighted_score
    FROM census_geographies g
    LEFT JOIN census_summary_levels sl ON g.summary_level_code = sl.code
    WHERE g.name % search_term OR g.name ILIKE '%' || search_term || '%'
    ORDER BY weighted_score DESC
    LIMIT max_results;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION search_census_geographies_by_summary_level(
    search_term TEXT,
    level_code TEXT,
    max_results INTEGER DEFAULT 10
)
RETURNS TABLE (
    id              INTEGER,
    name            VARCHAR(500),
    summary_level_name VARCHAR(255),
    latitude        DOUBLE PRECISION,
    longitude       DOUBLE PRECISION,
    for_param       VARCHAR(255),
    in_param        VARCHAR(255),
    weighted_score  REAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        g.id,
        g.name,
        sl.name AS summary_level_name,
        g.latitude,
        g.longitude,
        g.for_param,
        g.in_param,
        SIMILARITY(g.name, search_term)::real AS weighted_score
    FROM census_geographies g
    LEFT JOIN census_summary_levels sl ON g.summary_level_code = sl.code
    WHERE g.summary_level_code = level_code
      AND (g.name % search_term OR g.name ILIKE '%' || search_term || '%')
    ORDER BY weighted_score DESC
    LIMIT max_results;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION search_census_summary_levels(
    search_term TEXT,
    max_results INTEGER DEFAULT 1
)
RETURNS TABLE (
    code VARCHAR(3),
    name VARCHAR(255)
) AS $$
BEGIN
    RETURN QUERY
    SELECT r.code, r.name FROM (
        -- Exact code match (left-pad to 3 digits)
        SELECT sl.code, sl.name, 1.0::real AS score
        FROM census_summary_levels sl
        WHERE sl.code = LPAD(search_term, 3, '0')
        UNION ALL
        -- Exact name match (case-insensitive)
        SELECT sl.code, sl.name, 0.99::real AS score
        FROM census_summary_levels sl
        WHERE LOWER(sl.name) = LOWER(search_term)
        UNION ALL
        -- Fuzzy name match
        SELECT sl.code, sl.name, SIMILARITY(sl.name, search_term)::real AS score
        FROM census_summary_levels sl
        WHERE SIMILARITY(sl.name, search_term) > 0.3
    ) r
    ORDER BY r.score DESC
    LIMIT max_results;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION search_census_data_tables(
    p_table_id TEXT DEFAULT NULL,
    p_label_query TEXT DEFAULT NULL,
    p_api_endpoint TEXT DEFAULT NULL,
    p_limit INTEGER DEFAULT 20
)
RETURNS TABLE (
    data_table_id   VARCHAR(40),
    label           TEXT,
    component       VARCHAR(500),
    datasets        JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH matched_tables AS (
        SELECT DISTINCT
            dt.id,
            dt.data_table_id,
            dt.label,
            c.label AS component_label,
            GREATEST(
                CASE WHEN p_label_query IS NOT NULL THEN SIMILARITY(dt.label, p_label_query) ELSE 0 END,
                CASE WHEN p_label_query IS NOT NULL THEN MAX(SIMILARITY(dtd.label, p_label_query)) ELSE 0 END
            ) AS relevance
        FROM census_data_tables dt
        JOIN census_data_table_datasets dtd ON dtd.data_table_id = dt.id
        JOIN census_datasets ds ON dtd.dataset_id = ds.id
        LEFT JOIN census_components c ON ds.component_id = c.id
        WHERE
            (p_table_id IS NULL OR dt.data_table_id = p_table_id OR dt.data_table_id ILIKE p_table_id || '%')
            AND (p_label_query IS NULL OR dt.label % p_label_query OR dtd.label % p_label_query)
            AND (p_api_endpoint IS NULL OR ds.api_endpoint ILIKE '%' || p_api_endpoint || '%'
                 OR c.api_endpoint ILIKE '%' || p_api_endpoint || '%')
        GROUP BY dt.id, dt.data_table_id, dt.label, c.label
        ORDER BY relevance DESC, dt.data_table_id ASC
        LIMIT p_limit
    ),
    dataset_map AS (
        SELECT
            mt.data_table_id,
            mt.label,
            mt.component_label,
            jsonb_object_agg(
                COALESCE(ds.year::text, 'unknown'),
                ds.api_endpoint
            ) AS datasets
        FROM matched_tables mt
        JOIN census_data_table_datasets dtd ON dtd.data_table_id = (
            SELECT id FROM census_data_tables WHERE census_data_tables.data_table_id = mt.data_table_id
        )
        JOIN census_datasets ds ON dtd.dataset_id = ds.id
        GROUP BY mt.data_table_id, mt.label, mt.component_label
    )
    SELECT
        dm.data_table_id,
        dm.label,
        dm.component_label,
        dm.datasets
    FROM dataset_map dm;
END;
$$ LANGUAGE plpgsql;
