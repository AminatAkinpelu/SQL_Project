select * from world_layoffs.layoffs_staging5;

select max(total_laid_off), max(percentage_laid_off)
from world_layoffs.layoffs_staging5;

select * from world_layoffs.layoffs_staging5 
where percentage_laid_off = 1 
order by total_laid_off desc;

select * from world_layoffs.layoffs_staging5 
where percentage_laid_off = 1 
order by funds_raised_millions desc;

select company, sum(total_laid_off)
from world_layoffs.layoffs_staging5 
group by company order by 2 desc;

select min(`date`), max(`date`)
from world_layoffs.layoffs_staging5;

select industry, sum(total_laid_off)
from world_layoffs.layoffs_staging5 
group by industry order by 2 desc;

select year(`date`), sum(total_laid_off)
from world_layoffs.layoffs_staging5 
group by year(`date`) order by 1 desc;

select stage, sum(total_laid_off)
from world_layoffs.layoffs_staging5 
group by stage order by 1 desc;

select company, sum(total_laid_off)
from world_layoffs.layoffs_staging5 
group by company order by 2 desc;

select substring(`date`, 6, 2) as `MONTH`
from world_layoffs.layoffs_staging5;

select substring(`date`, 1, 7) as `MONTH`, sum(total_laid_off)
from world_layoffs.layoffs_staging5 
where substring(`date`, 1, 7) is not null
group by `MONTH`
order by 1 asc ;


with Rolling_Total as 
(
select substring(`date`, 1, 7) as `MONTH`, sum(total_laid_off) as total_off
from world_layoffs.layoffs_staging5 
where substring(`date`, 1, 7) is not null
group by `MONTH`
order by 1 asc
)
select `MONTH`, total_off, 
sum(total_off) over(order by `MONTH`) as rolling_total
from Rolling_Total;

select company, year(`date`),
sum(total_laid_off)
from world_layoffs.layoffs_staging5 
group by company, year(`date`) 
order by company asc;


with Company_Year (company, years, total_laid_off) as
(
select company, YEAR(`date`),
sum(total_laid_off)
from world_layoffs.layoffs_staging5 
group by company, YEAR(`date`) 
order by company asc
)
select *, 
dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from Company_Year where years is not null
order by Ranking asc;


with Company_Year (company, years, total_laid_off) as
(
select company, YEAR(`date`),
sum(total_laid_off)
from world_layoffs.layoffs_staging5 
group by company, YEAR(`date`) 
order by company asc
), Company_Year_Rank as 
(select *, 
dense_rank() over (partition by years order by total_laid_off desc) as Ranking
from Company_Year where years is not null
)
select * from Company_Year_Rank
where Ranking <= 5
;






















