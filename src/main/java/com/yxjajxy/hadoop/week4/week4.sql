-- 创建数据库
CREATE DATABASE IF NOT EXISTS yangjingyu;

-- 切换数据库
use yangjingyu;

-- 创建 t_user，并导入数据
CREATE TABLE IF NOT EXISTS t_user(
    UserID INT,
    Sex STRING,
    Age INT,
    Occupation INT,
    Zipcode STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES("field.delim"="::");

load data local inpath '/root/hadoop/users.dat' overwrite into table t_user;

-- 创建 t_movie，并导入数据
CREATE TABLE IF NOT EXISTS t_movie(
    MovieID INT,
    MovieName STRING,
    MovieType STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="::");

load data local inpath '/root/hadoop/movies.dat' overwrite into table t_movie;

-- 创建 t_rating，并导入数据
CREATE TABLE IF NOT EXISTS t_rating(
    UserID INT,
    MovieID INT,
    Rate INT,
    Times String
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH SERDEPROPERTIES ("field.delim"="::");

load data local inpath '/root/hadoop/ratings.dat' overwrite into table t_rating;

-- 题目一：展示电影 ID 为 2116 这部电影各年龄段的平均影评分
select u.Age, avg(r.Rate)
from (
    select UserID, Rate
    from t_rating
    where MovieID = 2116
) r join t_user u on (r.UserID = u.UserID)
group by u.Age;

-- 题目二：找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数
select m.MovieName, res.avgrate, res.total
from (
         select r.MovieID, avg(r.Rate) as avgrate, count(*) as total
         from t_rating r left join t_user u on (r.UserID = u.UserID)
         where u.Sex = 'M'
         group by r.MovieID
         having total > 50
         order by avgrate desc limit 10
) res left join t_movie m on (res.MovieID = m.MovieID);

-- 题目三：找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分
select m.MovieName, r.avgrate
from (
         select m.MovieID, avg(r.Rate) as avgrate
         from (
                  select r.MovieID, r.Rate as seq
                  from (
                           select r.UserID
                           from t_rating r left join t_user u on (r.UserID = u.UserID)
                           where u.Sex = 'F'
                           group by r.UserID
                           order by count(*) desc limit 1
                       ) u1 left join t_rating r on (u1.UserID = r.UserID)
                  order by r.Rate desc limit 10
         ) m left join t_rating r on (m.MovieID = r.MovieID)
         group by m.MovieID
         order by avg(m.seq) desc
) r left join t_movie m on (r.MovieID = m.MovieID);