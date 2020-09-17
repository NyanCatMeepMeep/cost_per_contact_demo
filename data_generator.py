import random
import mysql.connector

employees = ['Sam','Bob','Mary','Jane','Kyle','Stan']
contact_types = ['compromised','game-crash','payment','2-factor']
day_of_week = ['Mon','Tues','Wed','Thurs','Fri']

cnx = mysql.connector.connect(user='root', password='nyan',
                          host='127.0.0.1')
cursor = cnx.cursor()

def mysql_db_setup():
	create_db = '''create database if not exists cpc_gen_data;'''
	use_db = '''use cpc_gen_data;'''
	create_issue_table = '''create table if not exists issue_data_raw (issue_id int, contact_reason varchar(30), day_of_week varchar(6), employee varchar(5), worktime int);'''
	clean_table_if_exists = '''delete from issue_data_raw;'''
	safe_mode_off = '''SET SQL_SAFE_UPDATES=0;'''

	cursor.execute(create_db)
	cnx.commit()
	cursor.execute(use_db)
	cursor.execute(create_issue_table)
	cursor.execute(clean_table_if_exists)
	cnx.commit()
	cursor.execute(safe_mode_off)


def populating_data():
	# num_contacts_by_type_by_day
	ncbtbd = {}
	# num_contacts_by_type_by_day_by_employee
	ncbtbdbe = {}

	#Genrating total contact counts per category by day
	for c in contact_types:
		for d in day_of_week:
			ncbtbd[str(c)+','+str(d)] = random.randint(20,175)

	for cbtbd in ncbtbd:
		# print("The value in the ncbtbd dictionary for the key of "+str(cbtbd)+" has a value of "+str(ncbtbd[cbtbd]))
		max_allowed = int(int(ncbtbd[cbtbd])*.36) #formerly 56%, 46%
		# print("The max allowed in the randint is "+str(max_allowed))
		for e in employees:
			ncbtbdbe[str(cbtbd)+','+str(e)] = random.randint(0,max_allowed)
			# print("Out of a possible count of "+str(ncbtbd[cbtbd])+" issues for the value of "+str(cbtbd)+", "+str(e)+" will be assigned to work "+str(random.randint(0,max_allowed)))

	for x in ncbtbdbe:
		str_x = str(x)
		split_str_x = str_x.split(',')
		num_rows = ncbtbdbe[x]
		if num_rows == 0:
			pass
		else:
			on_row = 0
			insert_command = '''insert into issue_data_raw values '''
			values_str = ''
			while on_row < num_rows:
				issue_id = random.randint(1111,99999)
				if split_str_x[0] == 'compromised':
					ht = random.randint(180,480) #3 - 8 min
				if split_str_x[0] == 'game-crash':
					ht = random.randint(180,360) # 3 - 6 min
				if split_str_x[0] == 'payment':
					ht = random.randint(300,600) # 5 - 10 min
				if split_str_x[0] == '2-factor':
					ht = random.randint(480,900) # 8 - 15 min

				insert_line = '('+str(issue_id)+', '+str(split_str_x).replace('[','').replace(']','')+', '+str(ht)+'),'
				values_str = values_str + insert_line
				on_row = on_row + 1

			insert = insert_command + values_str[:-1]
			cursor.execute(insert)
			cnx.commit()

def molding_data():
	drop_issue_data = '''drop table if exists issue_data;'''
	delete_issue_data = '''delete from issue_data;'''
	create_issue_data = '''create table if not exists issue_data
	(
	issue_id int,
	employee_id int,
	contact_type_id int,
	day_of_week_id int,
	issue_worktime int
	);'''

	drop_employee_data = '''drop table if exists employee_data;'''
	delete_employee_data = '''delete from employee_data;'''
	create_employee_data = '''create table if not exists employee_data
	(
	employee_id int,
	employee_name varchar(10),
	employee_hr_rate float
	);'''

	drop_contact_reasons = '''drop table if exists contact_reasons;'''
	delete_contact_reasons = '''delete from contact_reasons;'''
	create_contact_reasons = '''create table if not exists contact_reasons
	(
	contact_type_id int,
	contact_reason varchar(15)
	);'''

	drop_dates = '''drop table if exists dates;'''
	delete_dates = '''delete from dates;'''
	create_dates = '''create table if not exists dates
	(
	day_of_week_id int,
	day_of_week varchar(8)
	);'''


	insert_molded_data = '''insert into issue_data 
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
	from issue_data_raw;'''


	populate_employee_dim = '''insert into employee_data values
	(1, 'Sam',  12.5), 
	(2, 'Bob', 11.25), 
	(3, 'Mary', 13.1), 
	(4, 'Jane', 11.25), 
	(5, 'Kyle', 12.5), 
	(6, 'Stan', 12.75),
	(99, 'Unknown', 0);'''

	populate_contact_reasons = '''insert into contact_reasons values
	(1, 'compromised'), 
	(2, 'game-crash'), 
	(3, 'payment'), 
	(4, '2-factor'),
	(99, 'unknown');'''

	populate_dates = '''insert into dates values
	(1, 'Mon'),
	(2, 'Tues'),
	(3, 'Wed'),
	(4, 'Thurs'),
	(5, 'Fri'),
	(99, 'Unknown');'''

	cursor.execute(drop_issue_data)
	cursor.execute(create_issue_data)
	cursor.execute(drop_employee_data)
	cursor.execute(create_employee_data)
	cursor.execute(drop_contact_reasons)
	cursor.execute(create_contact_reasons)
	cursor.execute(drop_dates)
	cursor.execute(create_dates)
	cursor.execute(delete_issue_data)
	cursor.execute(delete_employee_data)
	cursor.execute(delete_contact_reasons)
	cursor.execute(delete_dates)
	cursor.execute(insert_molded_data)
	cnx.commit()
	cursor.execute(populate_employee_dim)
	cnx.commit()
	cursor.execute(populate_contact_reasons)
	cnx.commit()
	cursor.execute(populate_dates)
	cnx.commit()



mysql_db_setup()
populating_data()
molding_data()