from faker import Faker
import numpy as np
import pandas as pd
import random
import sys 
import string
import datetime
from collections import defaultdict

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
    rec = set()
    if static_values:
        if number_of_rows > len(static_values):
            print("Not enough values in static value list to form unique/PK records")
            sys.exit(1)
        for i in range(0,number_of_rows): 
            rec.add(static_values[i])
        
    else:
        for i in range(0,number_of_rows):
            if type_of_rec == 'number': 
                rec.add(faker.unique.random_int(start_range, end_range))
            elif type_of_rec == 'ID':
                id_rec = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(len_attr))
                while id_rec in rec:
                    id_rec = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(len_attr))
                rec.add(id_rec)
            elif type_of_rec == 'country prefix': 
                rec.add(faker.unique.country_code())
            elif type_of_rec == 'country':
                rec.add(faker.unique.country())
            elif type_of_rec == 'phone number':
                rec.add(faker.unique.phone_number()) 
            elif type_of_rec == 'city prefix':
                rec.add(faker.unique.city())
            elif type_of_rec == 'city':
                rec.add(faker.unique.city())
            elif type_of_rec == 'state prefix':
                rec.add(faker.unique.state())
            elif type_of_rec == 'state':
                rec.add(faker.unique.state())
            elif type_of_rec == 'postal code':
                rec.add(faker.unique.postalcode())
            elif type_of_rec == 'name':
                rec.add(faker.unique.name())
            elif type_of_rec == 'char' or type_of_rec == 'varchar':
                if type_of_rec == 'char':
                    char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(len_attr))
                    while char_rec in rec: 
                        char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(len_attr)) 
                    rec.add(char_rec)
                else:
                    rand_len = random.randint(0, len_attr)
                    var_char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(rand_len))
                    while var_char_rec in rec: 
                        var_char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(rand_len)) 
                    rec.add(var_char_rec)
            elif type_of_rec == 'first name':
                rec.add(faker.unique.first_name())
            elif type_of_rec == 'last name':
                rec.add(faker.unique.last_name())
            elif type_of_rec == 'decimal':
                rand_dec = (random.randint(start_range*(10**precision_dec),end_range*(10**precision_dec)))/(10**precision_dec)
                while rand_dec in rec:
                    rand_dec = (random.randint(start_range*(10**precision_dec),end_range*(10**precision_dec)))/(10**precision_dec)
                rec.add(rand_dec)
            elif type_of_rec == 'date':
                rec.add(faker.unique.date_between(start_range, end_range))
            elif type_of_rec == 'datetime':
                rec.add(faker.unique.date_time_between(start_range, end_range))
            elif type_of_rec == 'time':
                start_sec = start_range.hour*60*60 + start_range.minute*60 + start_range.second
                end_sec = end_range.hour*60*60 + end_range.minute*60 + end_range.second
                rand_sec = faker.unique.random_int(start_sec, end_sec)
                hour_time = rand_sec//(60*60)
                minute_time = (rand_sec - (hour_time*60*60)) // 60
                second_time = (rand_sec - (hour_time*60*60)) % 60
                time_rec = datetime.time(hour_time, minute_time, second_time)
                rec.add(time_rec)
    
    rec = list(rec)            
    return({column_name: rec}) 

def generate_attr(column_name, type_of_rec, start_range, end_range, static_values, len_attr, precision_dec, number_of_rows):
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
                if not len_attr:
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

def generate_composite_key(composite_list):
    comp_set = set()
    number_of_rows = composite_list[0][7]
    iiiii = 0
    for r in range(number_of_rows):
        iiiii += 1
        print(datetime.datetime.now(), " : Composite Key Generation. Processing Line : ", iiiii)
        curr_rec = []
        dupes_cnt = 0
        
        while tuple(curr_rec) in comp_set or not curr_rec: #check if the composite key combination already exists
            curr_rec = []
            dupes_cnt += 1
            
            if dupes_cnt > 5000:
                if dupes_cnt > len(comp_set):
                    print("Not enough combination for generating Composite Key. Number of attempts made: ", dupes_cnt,". Generated set size: ", len(comp_set))
                    sys.exit(1)
            
            for i in range(len(composite_list)): 
                if not composite_list[i][4]:
                    if composite_list[i][1] == 'ID': #type of record 
                        if composite_list[i][5] == composite_list[i][5]: #length of field
                            id_len = int(composite_list[i][5])
                        else:
                            id_len = 10
                        curr_rec.append(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(id_len)))
                    elif composite_list[i][1] == 'name': 
                        curr_rec.append(faker.name())
                    elif composite_list[i][1] == 'number':
                        curr_rec.append(faker.random_int(composite_list[i][2], composite_list[i][3]))
                    elif composite_list[i][1] == 'country prefix': 
                        curr_rec.append(faker.country_code())
                    elif composite_list[i][1] == 'country': 
                        curr_rec.append(faker.country())
                    elif composite_list[i][1] == 'phone number': 
                        curr_rec.append(faker.phone_number())
                    elif composite_list[i][1] == 'city prefix': 
                        curr_rec.append(faker.city())
                    elif composite_list[i][1] == 'city': 
                        curr_rec.append(faker.city())
                    elif composite_list[i][1] == 'state prefix': 
                        curr_rec.append(faker.state())
                    elif composite_list[i][1] == 'state': 
                        curr_rec.append(faker.state())
                    elif composite_list[i][1] == 'postal code': 
                        curr_rec.append(faker.postalcode())
                    elif composite_list[i][1] == 'decimal': 
                        curr_rec.append(random.randint(composite_list[i][2]*(10**composite_list[i][6]),composite_list[i][3]*(10**composite_list[i][6]))/(10**composite_list[i][6]))
                    elif composite_list[i][1] == 'char' or composite_list[i][1] == 'varchar':
                        if not composite_list[i][5]: 
                            print("Character or varchar columns need a length specification")
                            sys.exit(1)
                        
                        if composite_list[i][1] == 'char':
                            char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(int(composite_list[i][5]))) 
                            curr_rec.append(char_rec) 
                        else:
                            rand_len = random.randint(0, composite_list[i][5])
                            var_char_rec = ''.join(random.choice(string.ascii_uppercase) for _ in range(int(rand_len)))
                            curr_rec.append(var_char_rec) 
                    elif composite_list[i][1] == 'date':
                        curr_rec.append(faker.date_between(composite_list[i][2], composite_list[i][3]))
                    elif composite_list[i][1] == 'datetime':
                        curr_rec.append(faker.date_time_between(composite_list[i][2], composite_list[i][3]))
                    elif composite_list[i][1] == 'time':
                        start_sec = composite_list[i][2].hour*60*60 + composite_list[i][2].minute*60 + composite_list[i][2].second
                        end_sec = composite_list[i][3].hour*60*60 + composite_list[i][3].minute*60 + composite_list[i][3].second
                        rand_sec = faker.random_int(start_sec, end_sec)
                        hour_time = rand_sec//(60*60)
                        minute_time = (rand_sec - (hour_time*60*60)) // 60
                        second_time = (rand_sec - (hour_time*60*60)) % 60
                        time_rec = datetime.time(hour_time, minute_time, second_time)
                        curr_rec.append(time_rec)  
                    else:
                        print("Unidenfied")
                else:
                    curr_rec.append(random.choice(composite_list[i][4])) 
        print(datetime.datetime.now(), " : Step 1 Composite Key Generation. Processing Line : ", iiiii)            
        comp_set.add(tuple(curr_rec))
        print(datetime.datetime.now(), " : Step 2 Composite Key Generation. Processing Line : ", iiiii)
    comp_dict = defaultdict(list)
    print(datetime.datetime.now(), " : Step 3 Composite Key Generation. Processing Line : ", iiiii)
    for cd in list(comp_set):
        for idx, e in enumerate(cd):
            comp_dict[composite_list[idx][0]].append(e)
    print(datetime.datetime.now(), " : Step 4 Composite Key Generation. Processing Line : ", iiiii)
    return comp_dict 

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
        ck = False          
    
    
    if row['Databasename'] != db_curr or row['Tablename'] != tbl_curr:
        n = int(row['Number_Of_Records'])
        tblename = db_curr + '_' + tbl_curr + '.csv'
        tbl_df.to_csv(tblename)
        df_lists.append(tbl_df)
        tbl_df = pd.DataFrame()
        db_curr = row['Databasename'] 
        tbl_curr = row['Tablename']
        pk = False
        ck = False
     
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
                
    elif row['Key'] == 'Composite Key' and ck == False:
        composite_cols_df = user_ip_df.loc[(user_ip_df['Key'] == 'Composite Key') & (user_ip_df['Databasename'] == db_curr) & (user_ip_df['Tablename'] == tbl_curr)] 
        if composite_cols_df.shape[0] < 2:
            print("There is only one column given as composite key. Use primary key if one column is the PK.")
            sys.exit(1)
        
        comp_list = list()
        for j, comp_row in composite_cols_df.iterrows():
            comp_min, comp_max, comp_static_value_list, comp_precision_val, comp_len_val =  derive_meta(comp_row['Type'], comp_row['Minimum'], comp_row['Maximum'], comp_row['Static_Value'], comp_row['Length'], comp_row['Precision'], n)
            comp_tmp_list = [comp_row['Column'], comp_row['Type'], comp_min, comp_max, comp_static_value_list, comp_len_val, comp_precision_val, n]
            comp_list.append(comp_tmp_list)

        
        ck_dict = generate_composite_key(comp_list)
        ck_df = pd.DataFrame.from_dict(ck_dict,orient='index').transpose()
        tbl_df = pd.concat([tbl_df,ck_df], axis=1)
        ck = True                 

    else:
        data_dict = generate_attr(row['Column'], row['Type'], val_min, val_max, static_value_list, row['Length'], precision_val, n)
        data_df = pd.DataFrame.from_dict(data_dict,orient='index').transpose()
        tbl_df[row['Column']] = data_df[row['Column']]

tblename = db_curr + '_' + tbl_curr + '.csv'        
tbl_df.to_csv(tblename)