# Netflix Content Analysis for Movies/TV Shows - End-to-End (ETL) Data Analysis Project using SQL

![Netflix Logo](https://github.com/GreaterHeight/Netflix_ETL_SQL_Project/blob/main/NetflixLogo.png)

## Overview
This project requires a thorough examination of Netflix's movie and TV show data using SQL. The purpose is to extract relevant insights and answer a variety of (20+) business questions using the dataset. This paper details the project's objectives, business challenges, solutions, findings, and conclusions. 

## Objectives

- Examine the distribution of content types (movies versus television shows).
- Determine the most frequent ratings for films and television shows.
- Organise and analyse content by release year, country, and duration.
- Analyse and categorise information using precise criteria and keywords.

## Dataset

Though the dataset for this project is sourced from the Kaggle dataset but **it's uploaded here:** [Netflix Dataset](https://greaterheight.academy/NetflixContents.csv)


## Get dataset, Create Database & Table
**Step 1:** Download the **dataset from:** [Netflix Dataset](https://greaterheight.academy/NetflixContent.csv)

**Step 2:** Open the NetflixContent.csv file and explore it to determine the column names.

**Step 3:** Execute the SQL code below:

```sql
CREATE DATABASE MoviesDB
GO

USE MoviesDB
GO

CREATE TABLE NetflixContent
(
	ShowID nvarchar(MAX) NOT NULL,
	Type nvarchar(MAX) NULL,
	Title nvarchar(MAX) NULL,
	Director nvarchar(MAX) NULL,
	Cast nvarchar(MAX) NULL,
	Country nvarchar(MAX) NULL,
	DateAdded nvarchar(MAX) NULL,
	ReleaseYear smallint NULL,
	Rating nvarchar(MAX) NULL,
	Duration nvarchar(MAX) NULL,
	ListedIn nvarchar(MAX) NULL,
	Description nvarchar(MAX) NULL
)
GO
```
**Result** 
- MoviesDB database will be created
- NetflixContent table will be created


**Step 4:** Execute the SQL code below. The code inserts all the records into NetflixContent

```sql
BULK INSERT NetflixContent
FROM 'C:\DataSource\NetflixContent.csv'		--specify the folder where you downloaded the file
WITH (
   FORMAT = 'CSV',
	FIELDTERMINATOR = ',',  -- Specifies the column delimiter
    ROWTERMINATOR = '\n',   -- Specifies the row terminator (newline character)
    FIRSTROW = 2,            -- Optional: Skips the header row if present
	TABLOCK
);
Go
```

**Step 5:** Execute the SQL code below. Make a backup copy of NetflixContent

```sql

--Before you do anything on the imported data, we make a backup copy of NetflixContent

SELECT * INTO NetflixContent_stagging
FROM NetflixContent
```

## Dataset Cleaning 

### Step 1. Handle Missing Values

```sql
--Handle Missing Values
UPDATE NetflixContent_stagging
SET director = ISNULL(director, 'Unknown'),
    cast = ISNULL(cast, 'Unknown'),
    country = ISNULL(country, 'Unknown'),
    rating = ISNULL(rating, 'Not Rated'),
    duration = ISNULL(duration, 'Unknown');
```

### Step 2. Convert DateAded to proper DATE format

```sql
-- Since DateAdded column is NVARCHAR, change it to DATE
ALTER TABLE NetflixContent_stagging
ALTER COLUMN DateAded DATE;

-- NOTE: If some rows cannot be converted, force conversion first
UPDATE NetflixContent_stagging
SET date_added = TRY_CONVERT(DATE, DateAded);

```

### Step 3. Clean Text Fields (remove leading/trailing spaces)

```sql

UPDATE NetflixContent_stagging
SET title      = TRIM(Title),
    Director   = TRIM(Director),
    Cast       = TRIM(Cast),
    Country    = TRIM(Country),
    ListedIn  = TRIM(ListedIn);

```


**Create a User Defined-function (UDF) in TSQL to Remove Double Spaces within a string. This is necessary because there isn't any available to do so in TSQL**

```sql
CREATE FUNCTION dbo.RemoveDoubleSpaces (@input NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX)
    SET @result = @input

    -- Keep replacing double spaces until none left
    WHILE CHARINDEX('  ', @result) > 0
        SET @result = REPLACE(@result, '  ', ' ')

    RETURN @result
END
GO

```

**Now that the function has been created, we call RemoveDoubleSpaces on the columns with Update state as shown below**

```sql
UPDATE NetflixContent_stagging
SET title      = dbo.RemoveDoubleSpaces(Title),
    Director   = dbo.RemoveDoubleSpaces(Director),
    Cast       = dbo.RemoveDoubleSpaces(Cast),
    Country    = dbo.RemoveDoubleSpaces(Country),
    ListedIn  = dbo.RemoveDoubleSpaces(ListedIn)
```


### Step 4. Standardize inconsistent names, in our case - Country Names
```sql

UPDATE NetflixContent_Stagging
SET country = CASE 
                 WHEN Country IN ('USA', 'United States of America') THEN 'United States'
                 WHEN Country = 'UK' THEN 'United Kingdom'
                 ELSE Country
              END;

```

### Step 5. Convert the Initial Letter
It may be necessary in some instanes to clean the data by capilising the initial letter of each word in strings. TSQL does not provide such function
so we have to create a User-Defined Function - InitCap

```sql

CREATE FUNCTION dbo.InitCap (@str VARCHAR(MAX))
RETURNS VARCHAR(MAX)
AS
BEGIN
    DECLARE @result VARCHAR(MAX)
    SET @result = ''
    DECLARE @words TABLE (word VARCHAR(MAX))
    INSERT INTO @words
    SELECT value FROM STRING_SPLIT(@str, ' ')
    
    DECLARE @word VARCHAR(MAX)
    DECLARE cur CURSOR FOR SELECT word FROM @words
    OPEN cur
    FETCH NEXT FROM cur INTO @word
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @result = @result + UPPER(LEFT(@word, 1)) + LOWER(SUBSTRING(@word, 2, LEN(@word))) + ' '
        FETCH NEXT FROM cur INTO @word
    END
    CLOSE cur
    DEALLOCATE cur
    
    RETURN TRIM(@result)
END
GO
```

**Now that the InitCap UDF has been created, we call InitCap on the columns with Update state as shown below**

```sql

UPDATE NetflixContent_Stagging
SET  
Title = dbo.InitCap(Title),
Cast = dbo.InitCap(Cast),
Director = dbo.InitCap(Cast),
Cast = dbo.InitCap(Cast),
ListedIn = dbo.InitCap(ListedIn)

```

```sql
--Script to count null
SELECT 
    SUM(CASE WHEN [Type] IS NULL THEN 1 ELSE 0 END) AS [Type_NullCount],
    SUM(CASE WHEN [Title] IS NULL THEN 1 ELSE 0 END) AS [Title_NullCount],
    SUM(CASE WHEN [Director] IS NULL THEN 1 ELSE 0 END) AS [Director_NullCount],
    SUM(CASE WHEN [Cast] IS NULL THEN 1 ELSE 0 END) AS [Cast_NullCount],
    SUM(CASE WHEN [Country] IS NULL THEN 1 ELSE 0 END) AS [Country_NullCount],
    SUM(CASE WHEN [DateAdded] IS NULL THEN 1 ELSE 0 END) AS [Date_added_NullCount],
    SUM(CASE WHEN [ReleaseYear] IS NULL THEN 1 ELSE 0 END) AS [ReleaseYear_NullCount],
    SUM(CASE WHEN [Rating] IS NULL THEN 1 ELSE 0 END) AS [Rating_NullCount],
    SUM(CASE WHEN [Duration] IS NULL THEN 1 ELSE 0 END) AS [Duration_NullCount],
    SUM(CASE WHEN [ListedIn] IS NULL THEN 1 ELSE 0 END) AS [ListedIn_NullCount],
    SUM(CASE WHEN [Description] IS NULL THEN 1 ELSE 0 END) AS [Description_NullCount]
FROM NetflixContent_stagging

```


## Business Problems and Solutions

### 1. Display the total Number of Movies vs TV Shows

```sql

SELECT 
   type,
   COUNT(*) count_type
FROM NetflixContent_stagging
GROUP BY type

```

**Objective:** Determine the distribution of content types on Netflix.

### 2. Count the Number of Content Items in Each Genre

```sql

	SELECT 
		Trim(Value) AS genre,  
		COUNT(*) AS total_content  
	FROM NetflixContent_stagging
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
	FROM NetflixContent_stagging
	   CROSS APPLY string_split (country, ',') 
	GROUP BY Trim(Value)

) AS temp
WHERE country IS NOT NULL
ORDER BY total_content DESC
```

**Objective:** Identify the top 5 countries with the highest number of content items.

### 5. Find Content Added in the Last 5 Years

```sql

SELECT * from NetflixContent_stagging
WHERE DATEDIFF(Year, date_added, GetDate()) >=5
```

**Objective:** Retrieve content added to Netflix in the last 5 years.

### 6. List All Movies that are Documentaries

```sql
SELECT * 
FROM NetflixContent_stagging
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
FROM NetflixContent_stagging
WHERE cast LIKE '%Salman Khan%'
AND release_year > Year(GetDate()) - 10;
```

**Objective:** Count the number of movies featuring 'Salman Khan' in the last 10 years

### 9. Find the Top 10 Actors Who Have Appeared in the Highest Number of Movies Produced in India

```sql
SELECT TOP(10)
	Trim(Value) AS Actor,
	COUNT(*) HighestNumber
FROM NetflixContent_stagging
CROSS APPLY STRING_SPLIT(cast,',')
WHERE country LIKE '%India%' AND type = 'Movie'
GROUP BY Trim(Value)
Order BY COUNT(*) DESC
```

**Objective:** Identify the top 10 actors with the most appearances in Indian-produced movies.

### 10.  Categorize Content Based on the Presence of 'Kill' and 'Violence' Keywords 

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
    FROM NetflixContent_stagging
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
FROM NetflixContent_stagging
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
		NetflixContent_stagging
	CROSS APPLY STRING_SPLIT(director, ',')
) AS InnerTable
WHERE 
	director_name = 'Rajiv Chilaka';
```

**Objective:** List all content directed by 'Rajiv Chilaka'.

### 13. List All TV Shows with More Than 5 Seasons

**Versio/Method 1:** 

```sql
SELECT Title, 
	Trim(Value) Value
FROM NetflixContent_stagging
CROSS APPLY STRING_SPLIT(Duration, ' ',1)
WHERE TYPE = 'TV Show' AND Ordinal = 1
AND TRY_CAST(VALUE AS INT) > 5 
ORDER BY CAST(VALUE AS INT) DESC
```

**Versio/Method 2:** 
```sql
SELECT *
FROM NetflixContent_stagging
WHERE type = 'TV Show'
  AND TRY_CAST(LEFT(duration, CHARINDEX(' ', duration + ' ') - 1) AS INT) > 5;
```

**Objective:** Identify TV shows with more than 5 seasons.

### 14. Display content items added after August 20, 2021


```sql
SELECT * from NetflixContent_stagging where CAST(date_added AS DATE) >= '2021-08-20'
```

**Objective:** Display content items added after August 20, 2021

### 15. Display movies added on June 15, 2019

```sql
SELECT * from NetflixContent_stagging where type = 'Movie' AND CAST(date_added AS DATE) = '2019-06-15'

```

**Note:** If the date_added column in your table is stored as text (e.g., "June 15, 2019"), we can either:
- Compare it directly as a string, or
- Convert it into a DATE type using TRY_CONVERT for safer querying.

```sql
SELECT * FROM NetflixContent_stagging WHERE TRY_CONVERT(DATE, date_added, 107) = '2019-06-15';
```

### 15b. Show just the Count of movies added on June 15, 2019

```sql
	-- Show the count of movies added on June 15, 2019
	SELECT COUNT(*) AS MoviesAddedCount
	FROM NetflixContent_stagging
	WHERE TRY_CONVERT(DATE, date_added, 107) = '2019-06-15';
```


### 15c. Count of titles by Type (Movies vs TV Shows) on June 15, 2019

```sql
	-- Count of titles by Type (Movies vs TV Shows) on June 15, 2019
SELECT 
    type,
    COUNT(*) AS TitlesAddedCount
FROM NetflixContent_stagging
WHERE TRY_CONVERT(DATE, date_added, 107) = '2019-06-15'
GROUP BY type;
```


**Objective:** Display movies added on June 15, 2019


### 16. Display content items added in 2021

```sql
SELECT * from NetflixContent_stagging where Year(CAST(date_added AS DATE)) = 2021
```

**Objective:** Display content items added in 2021



### 17. Display movies added in 2021

```sql
	SELECT 
	* from 
	NetflixContent_stagging 
	Where type='Movie' AND CAST(date_added AS DATE) BETWEEN '2021-01-01' AND '2021-12-31'
```

**Objective:** Display movies added in 2021



### 18. Count the number of movies and tv series that each director has produced in different columns.


This version handles multiple directors in one row (using CROSS APPLY STRING_SPLIT), so if a title has "Director A, Director B", both get credited?


```sql
SELECT 
    TRIM(director_split.value) AS director,
    SUM(CASE WHEN nt.type = 'Movie' THEN 1 ELSE 0 END) AS MovieCount,
    SUM(CASE WHEN nt.type = 'TV Show' THEN 1 ELSE 0 END) AS TVShowCount
FROM NetflixContent_stagging nt
CROSS APPLY STRING_SPLIT(nt.director, ',') AS director_split
GROUP BY TRIM(director_split.value) 

```
**Objective:** Count the number of movies and tv series that each director has produced in different columns.


### 18a. Count the number of movies and tv series that each director has produced in different columns,  include a TotalCount column Movies + TV Shows) for each director

```sql
SELECT 
    LTRIM(RTRIM(director_split.value)) AS director,
    SUM(CASE WHEN nt.type = 'Movie' THEN 1 ELSE 0 END) AS MovieCount,
    SUM(CASE WHEN nt.type = 'TV Show' THEN 1 ELSE 0 END) AS TVShowCount,
    COUNT(*) AS TotalCount
FROM NetflixContent_stagging nt
CROSS APPLY STRING_SPLIT(nt.director, ',') AS director_split
WHERE nt.director IS NOT NULL
GROUP BY LTRIM(RTRIM(director_split.value))
ORDER BY TotalCount DESC, director;
```
**Objective:** Count the number of movies and tv series that each director has produced in different columns with  a TotalCount column Movies + TV Shows) for each director


### 18b. Count the number of movies and tv series that each director has produced in different columns, with TotalCount column (Movies + TV Shows) for each director, including percentage split of Movies vs TV Shows for each director.


```sql
SELECT 
    TRIM(director_split.value) AS director,
    SUM(CASE WHEN nt.type = 'Movie' THEN 1 ELSE 0 END) AS MovieCount,
    SUM(CASE WHEN nt.type = 'TV Show' THEN 1 ELSE 0 END) AS TVShowCount,
    COUNT(*) AS TotalCount,
    CAST(100.0 * SUM(CASE WHEN nt.type = 'Movie' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS MoviePercent,
    CAST(100.0 * SUM(CASE WHEN nt.type = 'TV Show' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS TVShowPercent
FROM NetflixContent_stagging nt
CROSS APPLY STRING_SPLIT(nt.director, ',') AS director_split
WHERE nt.director IS NOT NULL
GROUP BY TRIM(director_split.value)
ORDER BY TotalCount DESC, director;
```
**Objective:** Count the number of movies and tv series that each director has produced in different columns with  a TotalCount column Movies + TV Shows) for each director


### 18c. Count the number of movies and tv series that each director has produced in different columns, with TotalCount column (Movies + TV Shows) for each director, including percentage split of Movies vs TV Shows for each director. Show only the Top 5 directors ranked by their total number of titles

```sql
SELECT TOP 5
    TRIM(director_split.value) AS director,
    SUM(CASE WHEN nt.type = 'Movie' THEN 1 ELSE 0 END) AS MovieCount,
    SUM(CASE WHEN nt.type = 'TV Show' THEN 1 ELSE 0 END) AS TVShowCount,
    COUNT(*) AS TotalCount,
    CAST(100.0 * SUM(CASE WHEN nt.type = 'Movie' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS MoviePercent,
    CAST(100.0 * SUM(CASE WHEN nt.type = 'TV Show' THEN 1 ELSE 0 END) / COUNT(*) AS DECIMAL(5,2)) AS TVShowPercent
FROM NetflixContent_stagging nt
CROSS APPLY STRING_SPLIT(nt.director, ',') AS director_split
WHERE nt.director IS NOT NULL
GROUP BY TRIM(director_split.value)
ORDER BY TotalCount DESC, director;

```
**Objective:** Count the number of movies and tv series that each director has produced in different columns with  a TotalCount column Movies + TV Shows) for each director





### 19. Which country has the highest number of comedy movies?

**Version/Method 1:** 
This version handles without considering tie

```sql
 SELECT TOP 1
    country,
    COUNT(*) AS ComedyMovieCount        
FROM NetflixContent_stagging
WHERE type = 'Movie'
    AND listed_in LIKE '%comedies%'
    AND country IS NOT NULL
GROUP BY country
ORDER BY ComedyMovieCount DESC
```


**Version/Method 2:** 
This version finds the country (or countries, in case of a tie) with the highest number of comedy movies:

```sql
SELECT country, ComedyMovieCount
FROM (
    SELECT 
        country,
        COUNT(*) AS ComedyMovieCount,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
    FROM NetflixContent_stagging
    WHERE type = 'Movie'
      AND listed_in LIKE '%comedies%'
      AND country IS NOT NULL
    GROUP BY country
) ranked
WHERE rnk = 1;
```
**Objective:** Which country has the highest number of comedy movies?




### 20. For each year, which director has the maximum number of movies released

**Version/Method 1:** 
This version handles without considering tie
```sql

    SELECT 
        nt.release_year,
        TRIM(d.value) AS director,
        COUNT(*) AS MovieCount 
    FROM NetflixContent_stagging nt
    CROSS APPLY STRING_SPLIT(nt.director, ',') d
    WHERE nt.type = 'Movie'
      AND nt.director IS NOT NULL
      AND nt.release_year IS NOT NULL
    GROUP BY nt.release_year, TRIM(d.value)
```


**Version/Method 2:** 

```sql
SELECT release_year, director, MovieCount
FROM (
    SELECT 
        nt.release_year,
        TRIM(d.value) AS director,
        COUNT(*) AS MovieCount,
        MAX(COUNT(*)) OVER (PARTITION BY nt.release_year) AS MaxMoviesInYear
    FROM NetflixContent_stagging nt
    CROSS APPLY STRING_SPLIT(nt.director, ',') d
    WHERE nt.type = 'Movie'
      AND nt.director IS NOT NULL
      AND nt.release_year IS NOT NULL
    GROUP BY nt.release_year, TRIM(d.value)
) sub
WHERE MovieCount = MaxMoviesInYear
ORDER BY release_year, director;
```

**Objective:** For each year, which director has the maximum number of movies released



### 21. What is the average running length of movies in each genre?

```sql
SELECT 
    TRIM(g.value) AS Genre,
    COUNT(*) AS MovieCount,
    AVG(TRY_CAST(REPLACE(nt.duration, ' min', '') AS INT)) AS AvgRuntimeMinutes
FROM NetflixContent_stagging nt
CROSS APPLY STRING_SPLIT(nt.listed_in, ',') g
WHERE nt.type = 'Movie'
  AND nt.duration LIKE '%min%'
GROUP BY TRIM(g.value)
ORDER BY AvgRuntimeMinutes DESC;
```

**Objective:** What is the average running length of movies in each genre?


### 22. List directors who have directed both comedies and horror films.

```sql
SELECT 
    TRIM(d.value) AS Director,
    SUM(CASE WHEN TRIM(g.value) = 'Comedies' THEN 1 ELSE 0 END) AS ComedyCount,
    SUM(CASE WHEN TRIM(g.value) = 'Horror Movies' THEN 1 ELSE 0 END) AS HorrorCount
FROM NetflixContent_stagging nt
CROSS APPLY STRING_SPLIT(nt.director, ',') d
CROSS APPLY STRING_SPLIT(nt.listed_in, ',') g
WHERE nt.type = 'Movie'
  AND nt.director IS NOT NULL
  AND TRIM(g.value) IN ('Comedies', 'Horror Movies')
GROUP BY TRIM(d.value)
HAVING SUM(CASE WHEN TRIM(g.value) = 'Comedies' THEN 1 ELSE 0 END) > 0
   AND SUM(CASE WHEN TRIM(g.value) = 'Horror Movies' THEN 1 ELSE 0 END) > 0
ORDER BY Director;

```

**Objective:** List directors who have directed both comedies and horror films.



### 23. List the director's name and the number of horror and comedy films that he or she has directed.

```sql
SELECT 
    TRIM(d.value) AS Director,
    SUM(CASE WHEN TRIM(g.value) = 'Horror Movies' THEN 1 ELSE 0 END) AS HorrorCount,
    SUM(CASE WHEN TRIM(g.value) = 'Comedies' THEN 1 ELSE 0 END) AS ComedyCount
FROM NetflixContent_stagging nt
CROSS APPLY STRING_SPLIT(nt.director, ',') d
CROSS APPLY STRING_SPLIT(nt.listed_in, ',') g
WHERE nt.type = 'Movie'
  AND nt.director IS NOT NULL
GROUP BY TRIM(d.value)
HAVING SUM(CASE WHEN RTRIM(g.value) IN ('Horror Movies','Comedies') THEN 1 ELSE 0 END) > 0
ORDER BY Director;
```

**Objective:** List the director's name and the number of horror and comedy films that he or she has directed.




### 24. Find the Most Common Rating for Movies and TV Shows

```sql
WITH RatingCounts AS (
    SELECT 
        type,
        rating,
        COUNT(*) AS rating_count
    FROM NetflixContent_stagging
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
        CAST((SELECT COUNT(show_id) FROM NetflixContent_stagging WHERE country = 'India') AS numeric ) * 100, 2
    ) AS avg_release
FROM NetflixContent_stagging
WHERE country = 'India'
GROUP BY country, release_year
ORDER BY avg_release DESC
```

**Objective:** Calculate and rank years by the average number of content releases by India.




### 26. Calculate the average age of movies in the top 10 countries (by number of movies)

**Version 1:** 

```sql
SELECT TOP 10 
       TRIM(country_split.value), 
       AVG(YEAR(GETDATE()) - ReleaseYear) AS AvgMovieAge,
       COUNT(*) AS MovieCount
FROM NetflixContent_stagging nt
CROSS APPLY STRING_SPLIT(nt.country, ',') AS country_split
WHERE nt.type = 'Movie'
  AND nt.country IS NOT NULL
GROUP BY TRIM(country_split.value)
ORDER BY COUNT(*) DESC
go
```


**Version 2:** 
This version shows with ties included (so if multiple countries tie for 10th place, they all appear)

```sql
SELECT country, AvgMovieAge, MovieCount
FROM (
    SELECT 
        TRIM(country_split.value) AS country,
        AVG(YEAR(GETDATE()) - ReleaseYear) AS AvgMovieAge,
        COUNT(*) AS MovieCount,
        RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk
    FROM NetflixContent_stagging nt
    CROSS APPLY STRING_SPLIT(nt.country, ',') AS country_split
    WHERE nt.type = 'Movie'
      AND nt.country IS NOT NULL
    GROUP BY TRIM(country_split.value)
) ranked
WHERE rnk <= 10
ORDER BY MovieCount DESC, country;

```

**Objective:** Calculate the average age of movies in the top 10 countries (by number of movies)


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

