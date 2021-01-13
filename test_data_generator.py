from faker import Faker
import numpy as np
import pandas as pd
import random
import sys 
import string
import datetime

def derive_meta(type_of_rec, min_field,max_field,static_field,length_field, precision_field,  n):
    '''Setting up the missing data in the input'''
    if min_field != min_field:
        if type_of_rec not in ('date', 'time', 'datetime'):
            val_min = 0
        elif type_of_rec == 'date':
            val_min = datetime.date(1990,1,1)
        elif type_of_rec == 'time':
            val_min = datetime.time(0,0,0)
        else:
            val_min = datetime.datetime(1990,1,1,0,0,0)
    else:
        if type_of_rec not in ('date', 'time', 'datetime'):
            val_min = int(min_field)
        elif type_of_rec == 'date':
            date_splt = [int(rowval.strip()) for rowval in min_field.split('-')]
            val_min = datetime.date(date_splt[0],date_splt[1],date_splt[2])
        elif type_of_rec == 'time':
            time_splt = [int(rowval.strip()) for rowval in min_field.split(':')]
            val_min = datetime.time(time_splt[0],time_splt[1],time_splt[2])
        else:
            date_elem = (min_field.strip()).split(' ') [0]
            time_elem = (min_field.strip()).split(' ') [1] 
            date_splt = [int(rowval.strip()) for rowval in date_elem.split('-')]
            time_splt = [int(rowval.strip()) for rowval in time_elem.split(':')]
            val_min = datetime.datetime(date_splt[0], date_splt[1], date_splt[2], time_splt[0], time_splt[1], time_splt[2])            
        
    if max_field != max_field:
        if type_of_rec not in ('date', 'time', 'datetime'):
            if length_field != length_field:  
                val_max = val_min + 10*n
            else: 
                val_max = int(0.99999999999999 * (10**(int(length_field)))) 
        elif type_of_rec == 'date':
            val_max = datetime.date(2030,12,31)
        elif type_of_rec == 'time':
            val_max = datetime.time(23,59,59)
        else:
            val_max = datetime.datetime(2030,12,31,23,59,59)
    else:
        if type_of_rec not in ('date', 'time', 'datetime'): 
            val_max = int(max_field)
        elif type_of_rec == 'date':
            date_splt = [int(rowval.strip()) for rowval in max_field.split('-')] 
            val_max = datetime.date(date_splt[0],date_splt[1],date_splt[2]) 
        elif type_of_rec == 'time':
            time_splt = [int(rowval.strip()) for rowval in max_field.split(':')]
            val_max = datetime.time(time_splt[0],time_splt[1],time_splt[2])
        else:
            date_elem = (max_field.strip()).split(' ') [0]
            time_elem = (max_field.strip()).split(' ') [1] 
            date_splt = [int(rowval.strip()) for rowval in date_elem.split('-')]
            time_splt = [int(rowval.strip()) for rowval in time_elem.split(':')]
            val_max = datetime.datetime(date_splt[0], date_splt[1], date_splt[2], time_splt[0], time_splt[1], time_splt[2])
            
    if static_field != static_field: 
        static_value_list = []
    else: 
        if type_of_rec not in ('date', 'time', 'datetime'): 
            static_value_list = [rowval.strip() for rowval in static_field.split(',')] 
        elif type_of_rec == 'date' :
            static_value_str_list = [rowval.strip() for rowval in static_field.split(',')] 
            static_value_list = [d.date() for d in pd.to_datetime(static_value_str_list)]
        elif type_of_rec == 'time' :
            static_value_str_list = [rowval.strip() for rowval in static_field.split(',')] 
            static_value_list = [d.time() for d in pd.to_datetime(static_value_str_list)]
        else:
            static_value_str_list = [rowval.strip() for rowval in static_field.split(',')] 
            static_value_list = [d.datetime() for d in pd.to_datetime(static_value_str_list)]
        
    if precision_field != precision_field:
        precision_field = 2
    else:
        precision_field = int(precision_field)
        
    if length_field != length_field:
        length_field = 10
    else:
        length_field = int(length_field)
    
    return val_min, val_max, static_value_list, precision_field, length_field
    

def generate_key(column_name, type_of_rec, start_range, end_range, static_values, len_attr, precision_dec, number_of_rows):
    '''Generate values for records mentioned as primary key'''
    rec = []
    if static_values:
        if number_of_rows > len(static_values):
            print("Not enough values in static value list to form unique/PK records")
            sys.exit(1)
        for i in range(0,number_of_rows): 
            rec.append(static_values[i])
        
    else:
        for i in range(0,number_of_rows):
            if type_of_rec == 'number': 
                rec.append(faker.unique.random_int(start_range, end_range))
            elif type_of_rec == 'ID':
                id_rec = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(len_attr))
                while id_rec in rec:
                    id_rec = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(len_attr))
                rec.append(id_rec)
            elif type_of_rec == 'country prefix': 
                rec.append(faker.unique.country_code())
            elif type_of_rec == 'country':
                rec.append(faker.unique.country())
            elif type_of_rec == 'phone number':
                rec.append(faker.unique.phone_number()) 
            elif type_of_rec == 'city prefix':
                rec.append(faker.unique.city())
            elif type_of_rec == 'city':
                rec.append(faker.unique.city())
            elif type_of_rec == 'state prefix':
                rec.append(faker.unique.state())
            elif type_of_rec == 'state':
                rec.append(faker.unique.state())
            elif type_of_rec == 'postal code':
                rec.append(faker.unique.postalcode())
            elif type_of_rec == 'name':
                rec.append(faker.unique.name())
            elif type_of_rec == 'char' or type_of_rec == 'varchar':
                if type_of_rec == 'char':
                    char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(len_attr))
                    while char_rec in rec: 
                        char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(len_attr)) 
                    rec.append(char_rec)
                else:
                    rand_len = random.randint(0, len_attr)
                    var_char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(rand_len))
                    while var_char_rec in rec: 
                        var_char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(rand_len)) 
                    rec.append(var_char_rec)
            elif type_of_rec == 'first name':
                rec.append(faker.unique.first_name())
            elif type_of_rec == 'last name':
                rec.append(faker.unique.last_name())
            elif type_of_rec == 'decimal':
                rand_dec = (random.randint(start_range*(10**precision_dec),end_range*(10**precision_dec)))/(10**precision_dec)
                while rand_dec in rec:
                    rand_dec = (random.randint(start_range*(10**precision_dec),end_range*(10**precision_dec)))/(10**precision_dec)
                rec.append(rand_dec)
            elif type_of_rec == 'date':
                rec.append(faker.unique.date_between(start_range, end_range))
            elif type_of_rec == 'datetime':
                rec.append(faker.unique.date_time_between(start_range, end_range))
            elif type_of_rec == 'time':
                start_sec = start_range.hour*60*60 + start_range.minute*60 + start_range.second
                end_sec = end_range.hour*60*60 + end_range.minute*60 + end_range.second
                rand_sec = faker.unique.random_int(start_sec, end_sec)
                hour_time = rand_sec//(60*60)
                minute_time = (rand_sec - (hour_time*60*60)) // 60
                second_time = (rand_sec - (hour_time*60*60)) % 60
                time_rec = datetime.time(hour_time, minute_time, second_time)
                rec.append(time_rec)
                
    return({column_name: rec}) 

def generate_attr(column_name, type_of_rec, start_range, end_range, length_attr, static_values, len_attr, precision_dec, number_of_rows):
    '''Generate values for records which are not PK or FK'''
    rec = []
    #print(column_name, type_of_rec, start_range, end_range, length_attr, static_values, number_of_rows)
    if not static_values: 
        for i in range(0,number_of_rows): 
            if type_of_rec == 'country prefix': 
                rec.append(faker.country_code()) 
            elif type_of_rec == 'number': 
                rec.append(faker.random_int(start_range, end_range))
            elif type_of_rec == 'ID':
                if len_attr == len_attr:
                    id_len = int(len_attr)
                else:
                    id_len = 10
                rec.append(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(id_len)))
            elif type_of_rec == 'country': 
                rec.append(faker.country()) 
            elif type_of_rec == 'phone number': 
                rec.append(faker.phone_number())
            elif type_of_rec == 'city prefix':
                rec.append(faker.city())
            elif type_of_rec == 'city':
                rec.append(faker.city())
            elif type_of_rec == 'state prefix':
                rec.append(faker.state())
            elif type_of_rec == 'state':
                rec.append(faker.state())
            elif type_of_rec == 'postal code':
                rec.append(faker.postalcode())
            elif type_of_rec == 'name':
                rec.append(faker.name())
            elif type_of_rec == 'decimal':
                rec.append((random.randint(start_range*(10**precision_dec),end_range*(10**precision_dec)))/(10**precision_dec))
            elif type_of_rec == 'char' or type_of_rec == 'varchar':
                if not length_attr:
                    print("Character or varchar columns need a length specification")
                    sys.exit(1)
                
                if type_of_rec == 'char':
                    char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(int(len_attr))) 
                    rec.append(char_rec)
                else:
                    rand_len = random.randint(0, len_attr)
                    var_char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(int(rand_len)))
                    rec.append(var_char_rec)
            elif type_of_rec == 'date':
                rec.append(faker.date_between(start_range, end_range))
            elif type_of_rec == 'datetime':
                rec.append(faker.date_time_between(start_range, end_range))
            elif type_of_rec == 'time':
                start_sec = start_range.hour*60*60 + start_range.minute*60 + start_range.second
                end_sec = end_range.hour*60*60 + end_range.minute*60 + end_range.second
                rand_sec = faker.random_int(start_sec, end_sec)
                hour_time = rand_sec//(60*60)
                minute_time = (rand_sec - (hour_time*60*60)) // 60
                second_time = (rand_sec - (hour_time*60*60)) % 60
                time_rec = datetime.time(hour_time, minute_time, second_time)
                rec.append(time_rec)
                
    else: 
        for i in range(0,number_of_rows): 
            rec.append(random.choice(static_values))
                
    return({column_name: rec})             
           

user_ip_df = pd.read_excel('E:/TestDataGeneration/test_data_generation.xlsx', dtype=str)
faker = Faker(seed=1000)
tbl_df = pd.DataFrame()
df_lists = []
pk_dict = {}
for i, row in user_ip_df.iterrows():
        
    if i==0: #set up the first database and table
        db_curr = row['Databasename'] 
        tbl_curr = row['Tablename']
        n = int(row['Number_Of_Records'])
        pk = False           
    
    
    if row['Databasename'] != db_curr or row['Tablename'] != tbl_curr:
        n = int(row['Number_Of_Records'])
        tblename = db_curr + '_' + tbl_curr + '.csv'
        tbl_df.to_csv(tblename)
        df_lists.append(tbl_df)
        tbl_df = pd.DataFrame()
        db_curr = row['Databasename'] 
        tbl_curr = row['Tablename']
        pk = False
     
    print(datetime.datetime.now(), " : ", db_curr.strip() + "." + tbl_curr.strip() + "." + row['Column'].strip() )
    val_min, val_max, static_value_list, precision_val, len_val =  derive_meta(row['Type'], row['Minimum'], row['Maximum'], row['Static_Value'], row['Length'], row['Precision'], n)
       
    if row['Key'] == 'Primary Key' or row['Key'] == 'Unique':
        if row['Key'] == 'Primary Key' and pk:
            print(db_curr + '_' + tbl_curr + ':', "More than one column is specified as primary key. If using composite key, select the option for it")
            sys.exit(1)
        if row['Key'] == 'Primary Key': 
            pk = True
        data_dict = generate_key(row['Column'], row['Type'], val_min, val_max, static_value_list, len_val, precision_val, n)
        if row['Key'] == 'Primary Key':             
            pk_dict[row['Index']] =  list(data_dict.values())[0] 
        data_df = pd.DataFrame.from_dict(data_dict,orient='index').transpose()
        tbl_df[row['Column']] = data_df[row['Column']]
    
    elif row['Key'] == 'Foreign Key':
        if row['Dependency Index']:
            if row['Index'] > row['Dependency Index']: 
                if row['Dependency Index'] not in pk_dict:
                    print("Build the dependency before using it in the FK")
                    sys.exit(1)
                fk_list = [ random.choice(pk_dict[row['Dependency Index']]) for _ in range(n) ]
                tbl_df[row['Column']] = pd.Series(fk_list)

        else:
            print("For a foreign key, please specify dependency")
            sys.exit(1)

    else:
        data_dict = generate_attr(row['Column'], row['Type'], val_min, val_max, row['Length'], static_value_list, row['Length'], precision_val, n)
        data_df = pd.DataFrame.from_dict(data_dict,orient='index').transpose()
        tbl_df[row['Column']] = data_df[row['Column']]

tblename = db_curr + '_' + tbl_curr + '.csv'        
tbl_df.to_csv(tblename)