use cpc_gen_data;
drop table issue_data_raw;
SET SQL_SAFE_UPDATES=0;
drop database cpc_gen_data;

select count(*) from issue_data_raw;
select distinct contact_reason from issue_data_raw;
select distinct employee from issue_data_raw;

drop table issue_data;
create table issue_data
(
issue_id int,
employee_id int,
contact_type_id int,
day_of_week_id int,
issue_worktime int
);

drop table employee_data;
create table employee_data
(
employee_id int,
employee_name varchar(10),
employee_hr_rate float
);

drop table contact_reasons;
create table contact_reasons
(
contact_type_id int,
contact_reason varchar(15)
);

drop table dates;
create table dates
(
day_of_week_id int,
day_of_week varchar(8)
);


insert into issue_data 
select
issue_id,
case when employee = 'Sam' then 1
	when employee = 'Bob' then 2
    when employee = 'Mary' then 3
    when employee = 'Jane' then 4
    when employee = 'Kyle' then 5
    when employee = 'Stan' then 6
    else 99
end as employee_id,
case when contact_reason = 'compromised' then 1
	when contact_reason = 'game-crash' then 2
    when contact_reason = 'payment' then 3
    when contact_reason = '2-factor' then 4
    else 99
end as contact_type_id,
case when day_of_week = 'Mon' then 1
	when day_of_week = 'Tues' then 2
    when day_of_week = 'Wed' then 3
    when day_of_week = 'Thurs' then 4
    when day_of_week = 'Fri' then 5
    else 99
end as day_of_week,
worktime as issue_worktime
from issue_data_raw;


insert into employee_data values
(1, 'Sam',  12.5), 
(2, 'Bob', 11.25), 
(3, 'Mary', 13.1), 
(4, 'Jane', 11.25), 
(5, 'Kyle', 12.5), 
(6, 'Stan', 12.75),
(99, 'Unknown', 0);

insert into contact_reasons values
(1, 'compromised'), 
(2, 'game-crash'), 
(3, 'payment'), 
(4, '2-factor'),
(99, 'unknown');

insert into dates values
(1, 'Mon'),
(2, 'Tues'),
(3, 'Wed'),
(4, 'Thurs'),
(5, 'Fri'),
(99, 'Unknown');



select
contact_reason,
format(min(issue_cost),2) as min_cost,
format(max(issue_cost),2) as max_cost,
format(avg(issue_cost),2) as avg_cost
from 
(
	select
	i.issue_id,
	cr.contact_reason,
	d.day_of_week,
	i.employee_id,
	ed.employee_name,
	ed.employee_hr_rate,
	i.issue_worktime,
	(((ed.employee_hr_rate/60)/60)*i.issue_worktime) as issue_cost
	from issue_data i
	inner join employee_data ed
		on ed.employee_id = i.employee_id
	inner join contact_reasons cr
		on cr.contact_type_id = i.contact_type_id
	inner join dates d
		on d.day_of_week_id = i.day_of_week_id
) agg_data
group by contact_reason