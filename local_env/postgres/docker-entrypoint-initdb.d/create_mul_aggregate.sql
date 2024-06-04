/*
We're creating this function because, in finance, it's very common to multiply a series to get a compound return or interest.
Credit to https://stackoverflow.com/questions/21623282/multiplynum-aggregate-function-in-postgresql
*/

CREATE FUNCTION mul_sfunc(anyelement, anyelement) RETURNS anyelement
   LANGUAGE sql AS 'SELECT $1 * coalesce($2, 1)';

CREATE AGGREGATE mul(anyelement) (
   STYPE = anyelement,
   INITCOND = 1,
   SFUNC = mul_sfunc,
   COMBINEFUNC = mul_sfunc,
   PARALLEL = SAFE
);