install spatial;
load spatial;

create table places as select * from 's3://overturemaps-us-west-2/release/2025-03-19.1/theme=places/type=place/part-00000*.parquet' limit 0;
copy places from 's3://overturemaps-us-west-2/release/2025-03-19.1/theme=places/type=place/*.parquet';
delete from places where geometry is null;
create index places_rtree on places using rtree (geometry);
attach 'overture.duckdb' as output;
copy from database memory to output;
