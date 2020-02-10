CREATE TABLE wetar.ad_hoc_query(
age int,
gender string,
occupation string
)STORED AS PARQUET;

INSERT OVERWRITE TABLE westar.ad_hoc_query
select age,gender,occupation from westar.u_user_u
left join u_data d on u.user_id=d.user_id
join u_item i on d.item_id=i.movie_id
where d.ration>3 and i.action=1
group by age,gender,occupation;