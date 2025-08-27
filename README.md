# Netflix Movies/TV Shows End-to-End Data Analysis Project using SQL

![Netflix Logo](https://github.com/GreaterHeight/Netflix_ELT_SQL_Project/blob/main/NetflixLogo.png)

## Overview
This project requires a thorough examination of Netflix's movie and TV show data using SQL. The purpose is to extract relevant insights and answer a variety of (20+) business questions using the dataset. This paper details the project's objectives, business challenges, solutions, findings, and conclusions. 

## Objectives

- Examine the distribution of content types (movies versus television shows).
- Determine the most frequent ratings for films and television shows.
- Organise and analyse content by release year, country, and duration.
- Analyse and categorise information using precise criteria and keywords.

## Dataset

Though the dataset for this project is sourced from the Kaggle dataset, but its uploaded here: Netflix_titles.csv 


## Business Problems and Solutions

### 1. Display the total Number of Movies vs TV Shows

```sql
SELECT 
   type,
   COUNT(*) count_type
FROM netflix_titles
GROUP BY type
```

**Objective:** Determine the distribution of content types on Netflix.



### 2. Count the Number of Content Items in Each Genre

```sql
SELECT 
	Trim(Value) AS genre,  
	COUNT(*) AS total_content  
FROM netflix_titles
   CROSS APPLY string_split (listed_in, ',') 
GROUP BY Trim(Value);
```

**Objective:** Objective: Count the number of content items in each genre.

### 3. List All Movies Released 2020

```sql
SELECT * 
FROM netflix
WHERE type = 'Movie' AND release_year = 2020;
```

**Objective:** Retrieve all movies released in a specific year.

### 4. Find the Top 5 Countries with the Most Content on Netflix

```sql
SELECT Top(5) * 
FROM
(
	SELECT 
	Trim(Value) AS country,  
	COUNT(*) AS total_content  
	FROM netflix_titles
	   CROSS APPLY string_split (country, ',') 
	GROUP BY Trim(Value)

) AS temp
WHERE country IS NOT NULL
ORDER BY total_content DESC
```

**Objective:** Identify the top 5 countries with the highest number of content items.

### 5. Find Content Added in the Last 5 Years

```sql
5.	
SELECT * from netflix_titles
WHERE DATEDIFF(Year, date_added, GetDate()) >=5
```

**Objective:** Retrieve content added to Netflix in the last 5 years.

### 6. List All Movies that are Documentaries

```sql
SELECT * 
FROM Netflix_titles
WHERE listed_in LIKE '%Documentaries';
```

**Objective:** Retrieve all movies classified as documentaries.

### 7. Find All Content Without a Director

```sql
SELECT * 
FROM netflix
WHERE director IS NULL;
```

**Objective:** List content that does not have a director.

### 8. Find How Many Movies Actor 'Salman Khan' Appeared in the Last 10 Years

```sql
SELECT * 
FROM netflix_titles
WHERE cast LIKE '%Salman Khan%'
AND release_year > Year(GetDate()) - 10;
```

**Objective:** Count the number of movies featuring 'Salman Khan' in the last 10 years

### 9. Find the Top 10 Actors Who Have Appeared in the Highest Number of Movies Produced in India

```sql
SELECT TOP(10)
	Trim(Value) AS Actor,
	COUNT(*) HighestNumber
FROM netflix_titles
CROSS APPLY STRING_SPLIT(cast,',')
WHERE country LIKE '%India%' AND type = 'Movie'
GROUP BY Trim(Value)
Order BY COUNT(*) DESC
```

**Objective:** Identify the top 10 actors with the most appearances in Indian-produced movies.

### 10.Categorize Content Based on the Presence of 'Kill' and 'Violence' Keywords 

```sql
SELECT 
    category,
    COUNT(*) AS content_count
FROM (
    SELECT 
        CASE 
            WHEN description LIKE '%kill%' OR description LIKE '%violence%' THEN 'Bad'
            ELSE 'Good'
        END AS category
    FROM netflix_titles
) AS categorized_content
GROUP BY category;
```

**Objective:** Categorize content as 'Bad' if it contains 'kill' or 'violence' and 'Good' otherwise. Count the number of items in each category.


### 11. Identify the Longest Movie

```sql
SELECT 
    Type, 
	title, 
	trim(Value) total_minute, 
	duration
FROM netflix_titles
CROSS APPLY string_split(duration, ' ', 1)
WHERE type = 'Movie' and Ordinal = 1
Order By CAST(trim(Value) AS INT) DESC
```

**Objective:** Find the movie with the longest duration.

### 12. Find All Movies/TV Shows by Director 'Rajiv Chilaka'

```sql
SELECT 
   *
FROM 
(
	SELECT 	
		*, 
		Trim(value) AS director_name
	FROM 
		netflix_titles
	CROSS APPLY STRING_SPLIT(director, ',')
) AS InnerTable
WHERE 
	director_name = 'Rajiv Chilaka';
```

**Objective:** List all content directed by 'Rajiv Chilaka'.

### 13. List All TV Shows with More Than 5 Seasons

```sql
SELECT Title, 
	Trim(Value) Value
FROM netflix_titles
CROSS APPLY STRING_SPLIT(Duration, ' ',1)
WHERE TYPE = 'TV Show' AND Ordinal = 1
AND TRY_CAST(VALUE AS INT) > 5 
ORDER BY CAST(VALUE AS INT) DESC
```

**Objective:** Identify TV shows with more than 5 seasons.

### 14. Display content items added after August 20, 2021


```sql
SELECT * from netflix_titles where CAST(date_added AS DATE) >= '2021-08-20'
```

**Objective:** Display content items added after August 20, 2021

### 15. Display movies added to on June 15, 2019

```sql
SELECT * from netflix_titles where type = 'Movie' AND CAST(date_added AS DATE) = '2019-06-15'
```

**Objective:** Display movies added to on June 15, 2019


### 16. Display content items added in 2021

```sql
SELECT * from netflix_titles where Year(CAST(date_added AS DATE)) = 2021
```

**Objective:** Display content items added in 2021



### 17. Display movies added in 2021

```sql
SELECT 
* from 
netflix_titles 
Where type='Movie' AND CAST(date_added AS DATE) BETWEEN '2021-01-01' AND '2021-12-31'
```

**Objective:** Display movies added in 2021



### 18. Count the number of movies and tv series that each director has produced in different columns.

```sql
SELECT 
    director,
    SUM(CASE WHEN type = 'Movie' THEN 1 ELSE 0 END) AS MovieCount,
    SUM(CASE WHEN type = 'TV Show' THEN 1 ELSE 0 END) AS TVShowCount
FROM dbo.netflix_titles
WHERE director IS NOT NULL
GROUP BY director
ORDER BY director;


```

**Objective:** Count the number of movies and tv series that each director has produced in different columns.



### 19. Which country has highest number of comedy movies?

```sql

```

**Objective:** Which country has highest number of comedy movies?




### 20. For each year, which director has maximum number of movies released

```sql


```

**Objective:** For each year, which director has maximum number of movies released



### 21.What is the average running length of movies in each genre?

```sql

```

**Objective:** What is the average running length of movies in each genre?


### 22. List directors who have directed both comedies and horror films.

```sql

```

**Objective:** List directors who have directed both comedies and horror films.



### 23. List the director's name and the number of horror and comedy films that he or she has directed.

```sql

```

**Objective:** List the director's name and the number of horror and comedy films that he or she has directed.




### 24. Find the Most Common Rating for Movies and TV Shows

```sql
WITH RatingCounts AS (
    SELECT 
        type,
        rating,
        COUNT(*) AS rating_count
    FROM netflix_titles
    GROUP BY type, rating
),
RankedRatings AS (
    SELECT 
        type,
        rating,
        rating_count,
        RANK() OVER (PARTITION BY type ORDER BY rating_count DESC) AS rank
    FROM RatingCounts
)
SELECT 
    type,
    rating AS most_frequent_rating
FROM RankedRatings
WHERE rank = 1;
```

**Objective:** Identify the most frequently occurring rating for each type of content.




### 25. Calculate and rank years by the average number of content releases by India.

```sql
SELECT TOP 5
    country,
    release_year,
    COUNT(show_id) AS total_release,
    ROUND(
        CAST(COUNT(show_id) AS numeric) /
        CAST((SELECT COUNT(show_id) FROM netflix_titles WHERE country = 'India') AS numeric ) * 100, 2
    ) AS avg_release
FROM netflix_titles
WHERE country = 'India'
GROUP BY country, release_year
ORDER BY avg_release DESC
```

**Objective:** Calculate and rank years by the average number of content releases by India.



## Findings and Conclusion

- **Content Distribution:** The dataset contains a diverse range of movies and TV shows with varying ratings and genres.
- **Common Ratings:** Insights into the most common ratings provide an understanding of the content's target audience.
- **Geographical Insights:** The top countries and the average content releases by India highlight regional content distribution.
- **Content Categorization:** Categorizing content based on specific keywords helps in understanding the nature of content available on Netflix.

This analysis provides a comprehensive view of Netflix's content and can help inform content strategy and decision-making.



## Author - GreaterHeight Academy

This project is part of our portfolio, showcasing the SQL skills essential for data analyst roles. If you have any questions, feedback, or would like to fraternize, feel free to get in touch!

### Stay Updated and Join the Community

For more content on SQL, data analysis, and other data-related topics, make sure to follow me on social media and join our community:

- **YouTube**: [Subscribe to my channel for tutorials and insights](https://www.youtube.com/@TheSegunSamuel)
- **Instagram**: [Follow me for daily tips and updates](https://www.instagram.com/TheSegunSamuel/)
- **LinkedIn**: [Connect with me professionally](https://www.linkedin.com/in/TheSegunSamuel)

