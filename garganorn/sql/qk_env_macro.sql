-- qk_env_macro.sql: DuckDB scalar macros for quadkey to envelope conversion.
-- These match the Python quadkey_to_bbox() in stages.py exactly (agreement
-- tested over 10,000 random quadkeys to 1e-9 -- see tests/test_covering.py).
--
-- Implementer note (y/y+1 asymmetry):
--   Tile y increases southward.  The envelope's north edge (lat_max, ymax in
--   ST_MakeEnvelope) uses qk_tile_y(qk) (tile's top row), while the south
--   edge (lat_min, ymin) uses qk_tile_y(qk)+1 (tile's bottom row).  Swapping
--   these expansions flips every envelope and fails the 10k-quadkey test.
--
-- DuckDB note: 2**n returns DOUBLE (all arithmetic here is floating-point).
-- sinh is written as (exp(x)-exp(-x))/2 (no built-in sinh required).

CREATE OR REPLACE MACRO qk_tile_x(qk) AS
    list_sum(list_transform(generate_series(1, length(qk)),
        i -> CASE WHEN qk[i] IN ('1', '3')
                  THEN 2 ** (length(qk) - i) ELSE 0.0 END));

CREATE OR REPLACE MACRO qk_tile_y(qk) AS
    list_sum(list_transform(generate_series(1, length(qk)),
        i -> CASE WHEN qk[i] IN ('2', '3')
                  THEN 2 ** (length(qk) - i) ELSE 0.0 END));

CREATE OR REPLACE MACRO qk_env(qk) AS
    ST_MakeEnvelope(
        qk_tile_x(qk) / (2.0 ** length(qk)) * 360.0 - 180.0,
        degrees(atan(
            (exp(pi() * (1.0 - 2.0 * (qk_tile_y(qk) + 1.0) / (2.0 ** length(qk))))
           - exp(-pi() * (1.0 - 2.0 * (qk_tile_y(qk) + 1.0) / (2.0 ** length(qk)))))
            / 2.0
        )),
        (qk_tile_x(qk) + 1.0) / (2.0 ** length(qk)) * 360.0 - 180.0,
        degrees(atan(
            (exp(pi() * (1.0 - 2.0 * qk_tile_y(qk) / (2.0 ** length(qk))))
           - exp(-pi() * (1.0 - 2.0 * qk_tile_y(qk) / (2.0 ** length(qk)))))
            / 2.0
        ))
    )
