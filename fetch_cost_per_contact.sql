select
i.issue_id,
cr.contact_reason,
concat(cast(d.day_of_week_id as char)," - ", d.day_of_week) as ranked_dow,
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