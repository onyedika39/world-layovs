-- Data Cleaning

SELECT *
FROM layovs;

-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows 


CREATE TABLE layovs_staging 
LIKE layovs;

SELECT *
FROM layovs_staging;

INSERT layovs_staging
SELECT *
FROM layovs;


SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM layovs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layovs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layovs_staging
WHERE company = 'Casper';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layovs_staging
)
DELETE
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layovs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


SELECT *
FROM layovs_staging2
WHERE row_num > 1;

INSERT INTO layovs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) AS row_num
FROM layovs_staging;

DELETE
FROM layovs_staging2
WHERE row_num > 1;

SELECT *
FROM layovs_staging2;

-- Standardizing data

SELECT company, TRIM(company)
FROM layovs_staging2;

UPDATE layovs_staging2
SET company =  TRIM(company);

SELECT DISTINCT industry
FROM layovs_staging2
;

UPDATE layovs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layovs_staging2
ORDER BY 1;

UPDATE layovs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united ststes%';

SELECT `date`
FROM layovs_staging2;

UPDATE layovs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

 ALTER TABLE layovs_staging2
 MODIFY COLUMN `date` DATE;
 
 SELECT *
FROM layovs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layovs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layovs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layovs_staging2
WHERE company LIKE 'Airbnb%';

SELECT t1.industry, t2.industry
FROM layovs_staging2 t1
JOIN layovs_staging2 t2
ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layovs_staging2 t1
JOIN layovs_staging2 t2
ON t1.company = t2.company
SET t1.industry =  t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT * 
FROM layovs_staging2;

 SELECT *
FROM layovs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layovs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layovs_staging2;


