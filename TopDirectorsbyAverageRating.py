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
