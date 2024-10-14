select * 
from layoffs;

create table layoffs_staging
like layoffs;

select * 
from layoffs_staging2;

insert layoffs_staging
select * from layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or Blank Values
-- 4. Remove Any Columns

-- REMOVING DUPLICATES
select company, industry, total_laid_off, percentage_laid_off, 'date', 
ROW_NUMBER() OVER(
partition by company, industry, total_laid_off, percentage_laid_off, 'date') as row_num
from layoffs_staging;

with duplicates_cte as
(
select company, location, industry, 
total_laid_off, percentage_laid_off, 'date', stage,
  country, funds_raised_millions,
row_number() over(
partition by company, location, industry,
 industry, total_laid_off, percentage_laid_off, 'date', stage,
  country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicates_cte
where row_num > 1;


select * from layoffs_staging where company = 'Casper';

ALTER TABLE layoffs_staging3
add COLUMN row_num VARCHAR(50);

select * from layoffs_staging;
CREATE TABLE layoffs_staging5 (
  company text,
  location text,
  industry text,
  total_laid_off int DEFAULT NULL,
  percentage_laid_off int DEFAULT NULL,
  `date` text,
  stage text,
  country text,
  funds_raised_millions int DEFAULT NULL,
  row_num int DEFAULT NULL);

insert into layoffs_staging3 
select *,
row_number() over(
partition by company, location, industry,
 industry, total_laid_off, percentage_laid_off, `date`, stage,
  country, funds_raised_millions) as row_num
from layoffs_staging;

SELECT * FROM world_layoffs.layoffs_staging5;

insert into layoffs_staging5 
select company, location, industry,
  total_laid_off, percentage_laid_off, `date`, stage,
  country, funds_raised_millions,
row_number() over(
partition by company, location, industry,
 industry, total_laid_off, percentage_laid_off, `date`, stage,
  country, funds_raised_millions) as row_num
from layoffs_staging;


SELECT * FROM layoffs_staging5
where row_num < 1;

delete
 FROM layoffs_staging5
where row_num > 1;

SELECT * FROM layoffs_staging5
where row_num < 1;

SELECT * FROM world_layoffs.layoffs_staging5;

-- STANDARDIZING DATA

select company, trim(company)
from world_layoffs.layoffs_staging5;

update world_layoffs.layoffs_staging5
set company = trim(company);

select distinct industry
from  world_layoffs.layoffs_staging5
order by 1;

select * from world_layoffs.layoffs_staging5
where industry like 'crypto%';

update world_layoffs.layoffs_staging5
set industry = 'crypto'
where industry like 'crypto%';

select distinct country, trim(trailing '.' from country)
from world_layoffs.layoffs_staging5
order by 1;

update world_layoffs.layoffs_staging5
set country = trim(trailing '.' from country)
where country like 'United States%';

select `date`, 
str_to_date(`date`, '%m/%d/%Y')
from world_layoffs.layoffs_staging5;

update world_layoffs.layoffs_staging5
set `date` = str_to_date(`date`, '%m/%d/%Y');

select `date` 
from world_layoffs.layoffs_staging5;

alter table world_layoffs.layoffs_staging5
modify column `date` date;

select * from world_layoffs.layoffs_staging5;

select * from world_layoffs.layoffs_staging5
where total_laid_off is null
and percentage_laid_off is null;

update world_layoffs.layoffs_staging5 
set industry = null 
where industry = '';




select * from world_layoffs.layoffs_staging5
where industry is null 
or industry = '';

select * from world_layoffs.layoffs_staging5 t1 
join world_layoffs.layoffs_staging5 t2 
	on t1.company = t2.company 
    and t1.location = t2.location
where t1.industry is null 
and t2.industry is not null;

select t1.industry, t2.industry
from world_layoffs.layoffs_staging5 t1 
join world_layoffs.layoffs_staging5 t2 
	on t1.company = t2.company 
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update world_layoffs.layoffs_staging5 t1 
join world_layoffs.layoffs_staging5 t2 
	on t1.company = t2.company 
    set t1.industry = t2.industry
    where t1.industry is null
and t2.industry is not null;

select * from world_layoffs.layoffs_staging5
where company like 'Bally%';

select * from world_layoffs.layoffs_staging5 
where total_laid_off is null 
and percentage_laid_off is null;

select * from world_layoffs.layoffs_staging5;

alter table world_layoffs.layoffs_staging5 
drop column row_num;

delete from world_layoffs.layoffs_staging5
where total_laid_off is null 
and percentage_laid_off is null;



