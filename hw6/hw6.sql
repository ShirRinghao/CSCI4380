CREATE OR REPLACE FUNCTION
    recommendation(inputseries integer array
               , topk int
	       , w1 float, w2 float, w3 float, w4 float)
    RETURNS varchar AS $$
    DECLARE
       result VARCHAR ;
       myrow RECORD ;
       rates FLOAT;
    BEGIN

    CREATE TABLE allSeries(
        seriesid varchar, a1 float, a2 float, a3 float, a4 FLOAT
    );
   result = '';
   INSERT INTO allSeries(seriesid) SELECT DISTINCT s.seriesid FROM series s where s.seriesid NOT IN (select input.seriesid FROM unnest(inputseries) as input(seriesid));
   update allSeries SET a1 = 0 where a1 IS NULL;
   update allSeries SET a2 = 0 where a2 IS NULL;
   update allSeries SET a3 = 0 where a3 IS NULL; 
   update allSeries SET a4 = 0 where a4 IS NULL; 
   
   --a1
   FOR myrow In (SELECT
       sc.seriesid as id
   FROM
        seriescategory sc
       , (SELECT              -- find input series category
           sc.category
       FROM
           seriescategory sc
           , (select input.seriesid as id FROM unnest(inputseries) as input(seriesid)) as input
       WHERE
           sc.seriesid = input.id) as sc1
   WHERE
       sc.category = sc1.category
   )
   LOOP
      update allSeries set a1 = a1 +1 where allSeries.seriesid = myrow.id::varchar;
   END LOOP;


  --a2
  FOR myrow In (
      SELECT DISTINCT sp.seriesid, count(sp.platform) as numplat 
      FROM series s, seriesonplatform sp
      WHERE s.seriesid = sp.seriesid
      GROUP BY sp.seriesid
  )
  Loop
      update allSeries set a2 = a2 + myrow.numplat where allSeries.seriesid = myrow.seriesid::varchar;
  END Loop;

  --a3
   FOR myrow In (SELECT
       sd.seriesid as id
   FROM
        seriesdirectors sd
       , (SELECT              -- find input series category
           sd.director
       FROM
           seriesdirectors sd
           , (select input.seriesid as id FROM unnest(inputseries) as input(seriesid)) as input
       WHERE
           sd.seriesid = input.id) as sd1
   WHERE
       sd.director = sd1.director
   )
   LOOP
      update allSeries set a3 = a3 + 1 where allSeries.seriesid = myrow.id::varchar;
   END LOOP;

   FOR myrow In (SELECT
       sc.seriesid as id
   FROM
        seriescast sc
       , (SELECT              -- find input series category
           sc.castname
       FROM
           seriescast sc
           , (select input.seriesid as id FROM unnest(inputseries) as input(seriesid)) as input
       WHERE
           sc.seriesid = input.id) as sc1
   WHERE
       sc.castname = sc1.castname
   )
   LOOP
      update allSeries set a3 = a3 + 1 where allSeries.seriesid = myrow.id::varchar;
   END LOOP;


   --a4
    CREATE table rotten(seriesid varchar, rating FLOAT);
    INSERT INTO rotten 
    SELECT
        s.seriesid
        , (s.rottentomatoes / 10.0) as rating
    FROM
        series s
    WHERE
        s.rottentomatoes IS NOT NULL
        and s.imdbrating IS NULL
    GROUP BY
        s.seriesid;

    CREATE table imdb(seriesid varchar, rating FLOAT);
    INSERT INTO imdb 
    SELECT
        s.seriesid
        , s.imdbrating as rating
    FROM
        series s
    WHERE
        s.imdbrating IS NOT NULL
        and s.rottentomatoes is NULL
    GROUP BY
        s.seriesid;

    CREATE table two(seriesid varchar, rating FLOAT);
    INSERT INTO two 
    SELECT
        s.seriesid
        , s.imdbrating + (s.rottentomatoes /10.0) as rating
    FROM
        series s
    WHERE
        s.imdbrating IS NOT NULL
        and s.rottentomatoes is NOT NULL
    GROUP BY
        s.seriesid;

   for myrow IN (select * from imdb UNION select * from rotten UNION select * from two)
   LOOP
      update allSeries set a4 = a4 + myrow.rating where allSeries.seriesid = myrow.seriesid::varchar;
   END LOOP;

   ALTER table allSeries ADD sum FLOAT;
   update allSeries SET sum =  a1 * w1 + a2 * w2 + a3 * w3 +  (a4 * w4)/2.0;
   --Testing the result:
   for myrow IN (SELECT
          s.title as name, alls.sum as total
       FROM
          series s
          , allSeries alls
       WHERE
          s.seriesid = alls.seriesid::integer
          ORDER BY alls.sum DESC LIMIT topk) 
   loop
      result = result || myrow.name || ' (' || myrow.total::NUMERIC(6,3) || ')' ||E'\n';
   end loop;

   DROP TABLE IF EXISTS allSeries;
   DROP TABLE IF EXISTS rotten;
   DROP TABLE IF EXISTS imdb;
   DROP TABLE IF EXISTS two;
   RETURN result ;
END ;
$$ LANGUAGE plpgsql ;
