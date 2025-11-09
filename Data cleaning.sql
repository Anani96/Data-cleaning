select * from layoffs;

create table layoffs_staging
like layoffs;

insert into layoffs_staging
select * from layoffs;

select * from layoffs_staging;

with duplicate_cte as
(
	select * , row_number() over (partition by company,location,industry,total_laid_off,percentage_laid_off,`date`, country) as row_num
    from layoffs_staging
)
select * from duplicate_cte
where row_num>1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select * , row_number() over (partition by company,location,industry,total_laid_off,percentage_laid_off,`date`, country) as row_num
    from layoffs_staging;

select * from layoffs_staging2
where row_num>1;

delete
from layoffs_staging2
where row_num>1;

select company,trim(company) from layoffs_staging2
where company=trim(company);

update layoffs_staging2
set company=trim(company);
select * from layoffs_staging2;

select distinct industry from layoffs_staging2
order by 1;

update layoffs_staging2
set industry='Crypto'
where industry like 'Crypto%';
select * from layoffs_staging2;

select distinct country from layoffs_staging2
order by 1;

select `date`,STR_TO_DATE(`date`,'%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`=STR_TO_DATE(`date`,'%m/%d/%Y');

alter table layoffs_staging2
modify column `date` DATE;

update layoffs_staging2
set country='United States'
where country like 'United States%';

delete from layoffs_staging2
where total_laid_off is Null and percentage_laid_off is null;

select s1.company,s1.location,s1.country,s1.industry,s2.industry from layoffs_staging2 as s1
join layoffs_staging2 as s2
on s1.company=s2.company
where (s1.industry ='' or s1.industry is null)
and s2.industry is not null;

update layoffs_staging2
set industry = null
where industry ='';

update layoffs_staging2 as s1
join layoffs_staging2 as s2
on s1.company=s2.company
set s1.industry = s2.industry
where (s1.industry ='' or s1.industry is null)
and s2.industry is not null;

select * from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;

