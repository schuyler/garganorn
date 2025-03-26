install spatial;
load spatial;
create table places as select * from 's3://fsq-os-places-us-east-1/release/dt=2025-03-06/places/parquet/places-00000.zstd.parquet' limit 0;
-- copy places from 's3://fsq-os-places-us-east-1/release/dt=2025-03-06/places/parquet/places-00000.zstd.parquet';
copy places from 's3://fsq-os-places-us-east-1/release/dt=2025-03-06/places/parquet/places-*.zstd.parquet';
delete from places where longitude = 0;
delete from places where latitude = 0;
delete from places where geom is null;
create index places_rtree on places using rtree (geom);
attach 'fsq-osp.duckdb' as output;
copy from database memory to output;
