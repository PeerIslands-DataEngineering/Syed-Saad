-- Problem 1: Top Directors by Average Rating
SELECT 
  Director,
  COUNT(*) AS movie_count,
  ROUND(AVG(Rating), 2) AS avg_rating
 FROM Movies
WHERE Rating IS NOT NULL
  GROUP BY Director
HAVING COUNT(*) >= 3
ORDER BY avg_rating DESC
  LIMIT 5;

-- Problem 2: Genre-wise Movie Count and Average Budget
SELECT 
   Genre,
  COUNT(*) AS total_movies,
   ROUND(AVG(Budget), 2) AS avg_budget
   FROM Movies
  WHERE Budget IS NOT NULL AND Budget > 0
    GROUP BY Genre
ORDER BY avg_budget DESC;

-- Problem 3: Most Frequent Content Ratings
SELECT 
  ContentRating,
  COUNT(*) AS count
FROM Movies GROUP BY ContentRatingORDER BY count DESC LIMIT 5;

-- Problem 4: Most Awarded Actor per Language
SELECT Language, LeadActor, total_awards FROM (
  SELECT 
    Language,
    LeadActor,
    SUM(Awards) AS total_awards,RANK() OVER (PARTITION BY Language ORDER BY SUM(Awards) DESC) AS rnk FROM Movies WHERE LeadActor IS NOT NULL AND Language IS NOT NULL AND Awards IS NOT NULL GROUP BY Language, LeadActor
)
WHERE rnk = 1;

-- Problem 5: Yearly Movie Output by Country (USA, after 2000)
SELECT 
  ReleaseYear,
  COUNT(*) AS movies_released
FROM Movies
WHERE Country = 'USA' AND ReleaseYear > 2000
GROUP BY ReleaseYear
ORDER BY ReleaseYear;

-- Problem 6: Running Total of Box Office Revenue per Year
SELECT 
  ReleaseYear,
  Title,
  BoxOffice,
  SUM(BoxOffice) OVER (PARTITION BY ReleaseYear ORDER BY BoxOffice DESC) AS running_total
FROM Movies
WHERE BoxOffice IS NOT NULL AND ReleaseYear IS NOT NULL;

-- Problem 7: Ranking Movies by Critic Rating per Genre
SELECT 
  Title,
  Genre,
  CriticRating,
  RANK() OVER (PARTITION BY Genre ORDER BY CriticRating DESC) AS rating_rank
FROM Movies
WHERE CriticRating IS NOT NULL AND Genre IS NOT NULL;

-- Problem 8: UDF to Classify Movie Length
CREATE OR REPLACE FUNCTION classify_length(duration INT64)
RETURNS STRING
AS (
  CASE 
    WHEN duration IS NULL THEN 'Unknown'
   WHEN duration < 90 THEN 'Short'
  WHEN duration BETWEEN 90 AND 150 THEN 'Medium'WHEN duration > 150 THEN 'Long'
    ELSE 'Unknown'
  END
);

-- Using the UDF
SELECT 
  Title,
  Duration,
  classify_length(Duration) AS length_category
FROM Movies;

-- Problem 9: Stored Procedure to Generate Genre Rating Summary
CREATE OR REPLACE PROCEDURE generate_genre_rating_summary()
BEGIN
  CREATE OR REPLACE TABLE genre_rating_summary AS
  SELECT 
    Genre,
    COUNT(*) AS total_movies,
    ROUND(AVG(Rating), 2) AS avg_rating
  FROM Movies
  WHERE Rating IS NOT NULL AND Genre IS NOT NULL
  GROUP BY Genre;
END;

-- Call the procedure
CALL generate_genre_rating_summary();

-- Problem 10: Best and Worst Rated Movies per Director
SELECT 
  Director,
  FIRST_VALUE(Title) OVER (PARTITION BY Director ORDER BY Rating DESC) AS best_movie,
  LAST_VALUE(Title) OVER (PARTITION BY Director ORDER BY Rating DESC 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS worst_movie
FROM Movies
WHERE Director IS NOT NULL AND Rating IS NOT NULL;
