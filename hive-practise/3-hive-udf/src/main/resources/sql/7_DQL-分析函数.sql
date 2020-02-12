CREATE TABLE IF NOT EXISTS order_detail (
  user_id STRING,
  device_id STRING,
  user_type STRING,
  price DECIMAL(10, 2),
  sales INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH '/user/hadoop-twq/hive-course/order_detail.txt' OVERWRITE INTO TABLE order_detail;


--分析函数的语法：
Function(arg1, ..., argn) OVER ([PARTITION BY <...>] [ORDER BY <...>] [<window_clause>])

--## COUNT、SUM、MIN、MAX、AVG
select
    user_id,
    user_type,
    sales,
    --默认为从起点到当前行
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc) AS sales_1,
    --从起点到当前行，结果与sales_1不同。
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sales_2,
    --当前行+往前3行
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS sales_3,
    --当前行+往前3行+往后1行
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS sales_4,
    --当前行+往后所有行
    sum(sales) OVER(PARTITION BY user_type ORDER BY sales asc ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS sales_5,
    --分组内所有行
    SUM(sales) OVER(PARTITION BY user_type) AS sales_6
from
    order_detail
order by
    user_type,
    sales,
    user_id;



--first_value与last_value
select
    user_id,
    user_type,
    sales,
    ROW_NUMBER() OVER(PARTITION BY user_type ORDER BY sales) AS row_num,
    first_value(user_id) over (partition by user_type order by sales desc) as max_sales_user,
    first_value(user_id) over (partition by user_type order by sales asc) as min_sales_user,
    last_value(user_id) over (partition by user_type order by sales desc) as curr_last_min_user,
    last_value(user_id) over (partition by user_type order by sales asc) as curr_last_max_user
from
    order_detail;

--## lead与lag
--LEAD(col,n,DEFAULT) 用于统计窗口内往下第n行值
--第一个参数为列名，第二个参数为往下第n行（可选，默认为1），第三个参数为默认值（当往下第n行为NULL时候，取默认值，如不指定，则为NULL）

--LAG(col,n,DEFAULT) 用于统计窗口内往上第n行值
--第一个参数为列名，第二个参数为往上第n行（可选，默认为1），第三个参数为默认值（当往上第n行为NULL时候，取默认值，如不指定，则为NULL）
select
    user_id,device_id,sales,
    lead(device_id) over (order by sales) as default_after_one_line,
    lag(device_id) over (order by sales) as default_before_one_line,
    lead(device_id,2) over (order by sales) as after_two_line,
    lag(device_id,2,'abc') over (order by sales) as before_two_line
from
    order_detail;

--## RANK、ROW_NUMBER、DENSE_RANK
select
    user_id,user_type,sales,
    RANK() over (partition by user_type order by sales desc) as r,
    ROW_NUMBER() over (partition by user_type order by sales desc) as rn,
    DENSE_RANK() over (partition by user_type order by sales desc) as dr
from
    order_detail;

-- ## NTILE
select
    user_type,sales,
    --分组内将数据分成2片
    NTILE(2) OVER(PARTITION BY user_type ORDER BY sales) AS nt2,
    --分组内将数据分成3片
    NTILE(3) OVER(PARTITION BY user_type ORDER BY sales) AS nt3,
    --分组内将数据分成4片
    NTILE(4) OVER(PARTITION BY user_type ORDER BY sales) AS nt4,
    --将所有数据分成4片
    NTILE(4) OVER(ORDER BY sales) AS all_nt4
from
    order_detail
order by
    user_type,
    sales;

--求取sale前20%的用户ID
select
    user_id
from
    (
        select
            user_id,
            NTILE(5) OVER(ORDER BY sales desc) AS nt
        from
            order_detail
    )A
where nt=1;

--## CUME_DIST、PERCENT_RANK
select
    user_id,user_type,sales,
    --没有partition,所有数据均为1组
    CUME_DIST() OVER(ORDER BY sales) AS cd1,
    --按照user_type进行分组
    CUME_DIST() OVER(PARTITION BY user_type ORDER BY sales) AS cd2
from
    order_detail;


select
    user_type,sales,
    --分组内总行数
    SUM(1) OVER(PARTITION BY user_type) AS s,
  --RANK值
  RANK() OVER(ORDER BY sales) AS r,
  PERCENT_RANK() OVER(ORDER BY sales) AS pr,
  --分组内
  PERCENT_RANK() OVER(PARTITION BY user_type ORDER BY sales) AS prg
from
    order_detail;





