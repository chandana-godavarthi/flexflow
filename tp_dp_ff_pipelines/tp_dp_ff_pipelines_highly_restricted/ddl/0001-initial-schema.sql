--assume that schema exists and "use <schema>" was run

--$location is replaced at execution time by the migration library with the 
--storage path passed to the migration library
--$schema is replaced at execution time by the schema name passed in
--other $values can be passed in using kwargs.  
--Read more about Migration library features and and details
--at https://github.com/procter-gamble/de-cf-ps-migration

CREATE TABLE IF NOT EXISTS $schema.foo (`id` INT, `name` STRING, `customer` STRING) LOCATION '$location/foo'; --noqa

-- this table maps your table(s) columns to sgks.  In this example by customer,
-- but use any columns to join you want 
CREATE TABLE IF NOT EXISTS $schema.rls_ad_lkp ( --noqa
    sgk INT
    , application_name STRING
    , permission_name STRING
    , permission_id STRING
    , logic STRING
    , customer STRING
) LOCATION '$location/rls_ad_lkp' --noqa

--define your rls column (in this example customer) mappings to sgks that
-- you have defined
--insert into $schema.rls_ad_lkp (sgk,application_name,permission_name,
--permission_id,logic,customer)
