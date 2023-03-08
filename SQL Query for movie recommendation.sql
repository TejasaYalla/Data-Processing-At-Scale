-- Query1
CREATE TABLE query1 AS 
SELECT 
  genres.name AS name, 
  COUNT(DISTINCT hasagenre.movieid) AS moviecount 
FROM 
  genres, 
  hasagenre 
WHERE 
  genres.genreid = hasagenre.genreid 
GROUP BY 
  genres.name;

-- Query2 
CREATE TABLE query2 AS 
SELECT 
  genres.name AS name, 
  AVG(ratings.rating) AS rating 
FROM 
  genres, 
  ratings, 
  hasagenre 
WHERE 
  ratings.movieid = hasagenre.movieid 
  and hasagenre.genreid = genres.genreid 
GROUP BY 
  genres.name;

-- Query3
CREATE TABLE query3 AS 
SELECT 
  movies.title AS title, 
  COUNT(ratings.rating) AS countofratings 
FROM 
  ratings 
  INNER JOIN movies ON ratings.movieid = movies.movieid 
GROUP BY 
  movies.title 
HAVING 
  COUNT(ratings.rating) >= 10;

-- Query4
CREATE TABLE query4 AS 
SELECT 
  DISTINCT movies.movieid AS movieid, 
  movies.title AS title 
FROM 
  genres, 
  hasagenre, 
  movies 
WHERE 
  hasagenre.movieid = movies.movieid 
  AND hasagenre.genreid = genres.genreid 
  AND genres.name LIKE 'Comedy';

-- Query5
CREATE TABLE query5 AS 
SELECT 
  movies.title AS title, 
  avg(rating) AS average 
FROM 
  ratings 
  INNER JOIN movies AS movies ON ratings.movieid = movies.movieid 
GROUP BY 
  movies.title;

-- Query6
CREATE TABLE query6 AS 
SELECT 
  AVG(rating) AS average 
FROM 
  ratings, 
  hasagenre, 
  genres 
WHERE 
  ratings.movieid = hasagenre.movieid 
  AND hasagenre.genreid = genres.genreid 
  AND genres.name LIKE 'Comedy';

-- Query7
CREATE TABLE query7 AS 
SELECT 
  AVG(rating) AS average 
FROM 
  ratings 
WHERE 
  movieid in (
    SELECT 
      DISTINCT hasagenre.movieid 
    FROM 
      hasagenre 
      INNER JOIN genres ON hasagenre.genreid = genres.genreid 
    WHERE 
      genres.name in ('Comedy', 'Romance') 
    GROUP BY 
      hasagenre.movieid 
    HAVING 
      COUNT(DISTINCT genres.name) = 2
  );

-- Query8
CREATE TABLE query8 AS 
SELECT 
  AVG(rating) AS average 
FROM 
  ratings 
WHERE 
  movieid in (
    SELECT 
      DISTINCT hasagenre.movieid 
    FROM 
      hasagenre 
      INNER JOIN genres ON hasagenre.genreid = genres.genreid 
    WHERE 
      genres.name LIKE 'Romance'
  ) 
  AND movieid not in (
    SELECT 
      DISTINCT hasagenre.movieid 
    FROM 
      hasagenre 
      INNER JOIN genres ON hasagenre.genreid = genres.genreid 
    WHERE 
      genres.name LIKE 'Comedy'
  );

-- Query9
CREATE TABLE query9 AS 
SELECT 
  movieid, 
  rating 
FROM 
  ratings 
WHERE 
  userid = :v1;