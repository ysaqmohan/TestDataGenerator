from faker import Faker
import numpy as np
import pandas as pd
import random
import sys 
import string
import datetime
from collections import defaultdict
import os.path
import sqlalchemy
import psycopg2

def write_to_target(db_name, tbl_name, wrt_df):
    '''Write the df to database if param file exist, else write to a file''' 
    connection = False
    
    if not os.path.isfile('params.txt'):
        print(datetime.datetime.now(), " : ", db_name.strip() + "." + tbl_name.strip() + " : Paramter file unavailable. Writing the data to csv file" )
        filename = db_name + '_' + tbl_name + '.csv'
        wrt_df.to_csv(filename, index=False,  sep='|')
    else:
        #connecting to DB 
        try:             
            engine = sqlalchemy.create_engine('postgres://postgres:Password@123@localhost:5432/test_data_db') 
            with engine.connect() as connection: 
                print("DB connection established: ", bool(connection))
                
                if not connection.dialect.has_schema(connection, db_name):
                    print("Schema undefined. Creating a new schema: ", db_name)
                    engine.execute(sqlalchemy.schema.CreateSchema(db_name))
                
            wrt_df.to_sql(tbl_name, con=engine, if_exists="replace", schema= db_name, index=False)
            
        except (Exception, psycopg2.Error) as error : 
            print ("Error while connecting to DB.", error)
            print(datetime.datetime.now(), " : ", db_name.strip() + "." + tbl_name.strip() + " : Writing the data to csv file" )
            filename = db_name + '_' + tbl_name + '.csv' 
            wrt_df.to_csv(filename)
            
        finally: 
            #closing database connection. 
            if(connection): 
                connection.close() 
                print("PostgreSQL connection is closed")


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
    

def generate_key(column_name, type_of_rec, start_range, end_range, static_values, len_attr, precision_dec, nullable_ind, number_of_rows):
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
                id_rec = ''.join(random.choices(string.ascii_uppercase + string.digits, k=len_attr))
                while id_rec in rec:
                    id_rec = ''.join(random.choices(string.ascii_uppercase + string.digits,  k=len_attr))
                rec.add(id_rec)
            elif type_of_rec == 'country prefix': 
                rec.add(faker.unique.country_code())
            elif type_of_rec == 'country':
                rec.add(faker.unique.country())
            elif type_of_rec == 'phone number':
                rec.add(faker.unique.phone_number()) 
            elif type_of_rec == 'city prefix':
                rec.add(faker.unique.city_suffix())
            elif type_of_rec == 'city':
                rec.add(faker.unique.city())
            elif type_of_rec == 'address':
                rec.add((faker.unique.address()).replace('\n', ', '))
            elif type_of_rec == 'state':
                rec.add(faker.unique.state())
            elif type_of_rec == 'postal code':
                rec.add(faker.unique.postalcode())
            elif type_of_rec == 'name':
                rec.add(faker.unique.name())
            elif type_of_rec == 'char' or type_of_rec == 'varchar':
                if type_of_rec == 'char':
                    char_rec = ''.join(random.choices(string.ascii_uppercase, k=len_attr))
                    while char_rec in rec: 
                        char_rec = ''.join(random.choices(string.ascii_uppercase, k=len_attr))
                    rec.add(char_rec)
                else:
                    rand_len = random.randint(0, len_attr)
                    var_char_rec = ''.join(random.choices(string.ascii_uppercase, k=rand_len))
                    while var_char_rec in rec:
                        rand_len = random.randint(0, len_attr)
                        var_char_rec = ''.join(random.choices(string.ascii_uppercase, k=rand_len))
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
            else:
                print("Value for type of record is unidentified. Exiting")
                sys.exit(1)
    
    rec = list(rec) 
    
    if nullable_ind != 'N': 
        percent_rand_null = random.randint(1,5) 
        number_of_nulls = n*percent_rand_null//100
        
        for null_r in range(0, number_of_nulls):
            rec[random.randint(0,n-1)] = None 
            
    return({column_name: rec}) 

def generate_attr(column_name, type_of_rec, start_range, end_range, static_values, len_attr, precision_dec, nullable_ind, number_of_rows):
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
                rec.append(''.join(random.choices(string.ascii_uppercase + string.digits, k = id_len)))
            elif type_of_rec == 'country': 
                rec.append(faker.country()) 
            elif type_of_rec == 'phone number': 
                rec.append(faker.phone_number())
            elif type_of_rec == 'city prefix':
                rec.append(faker.city_suffix())
            elif type_of_rec == 'city':
                rec.append(faker.city())
            elif type_of_rec == 'address':
                rec.append((faker.address()).replace('\n', ', '))
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
                    char_rec = ''.join(random.choices(string.ascii_uppercase, k=len_attr))
                    rec.append(char_rec)
                else:
                    rand_len = random.randint(0, len_attr)
                    var_char_rec = ''.join(random.choices(string.ascii_uppercase, k=rand_len))
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
                print("Value for type of record is unidentified. Exiting")
                sys.exit(1)
                
    else: 
        for i in range(0,number_of_rows): 
            rec.append(random.choice(static_values))
    
    if nullable_ind != 'N': 
        percent_rand_null = random.randint(1,5) 
        number_of_nulls = n*percent_rand_null//100
        
        for null_r in range(0, number_of_nulls):
            rec[random.randint(0,n-1)] = None
                
    return({column_name: rec})             

def generate_composite_key(composite_list):
    comp_set = set()
    number_of_rows = composite_list[0][7]
    for r in range(number_of_rows):
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
                        curr_rec.append(''.join(random.choices(string.ascii_uppercase + string.digits, k=id_len)))
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
                        curr_rec.append(faker.city_suffix())
                    elif composite_list[i][1] == 'city': 
                        curr_rec.append(faker.city())
                    elif composite_list[i][1] == 'address': 
                        curr_rec.append((faker.address()).replace('\n', ', '))
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
                        print("Unidentified type of record. Exiting")
                        sys.exit(1)
                else:
                    curr_rec.append(random.choice(composite_list[i][4]))            
        comp_set.add(tuple(curr_rec))
    comp_dict = defaultdict(list)
    
    for cd in list(comp_set):
        for idx, e in enumerate(cd):
            comp_dict[composite_list[idx][0]].append(e)
    print(datetime.datetime.now(), " : Composite Key Generation completed")
    return comp_dict 

user_ip_df = pd.read_excel('schema/test_data_generation.xlsx', dtype=str)
faker = Faker(seed=1000)
tbl_df = pd.DataFrame()
pk_dict = {}
lookup_lst = user_ip_df[(user_ip_df['Dependency Index'].notnull()) & (user_ip_df['Key'] != 'Foreign Key')] ['Dependency Index'].to_list()
lookup_dict = dict.fromkeys(lookup_lst,[])

for i, row in user_ip_df.iterrows():
        
    if i==0: #set up the first database and table
        db_curr = row['Databasename'] 
        tbl_curr = row['Tablename']
        n = int(row['Number_Of_Records'])
        pk = False 
        ck = False          
    
    
    if row['Databasename'] != db_curr or row['Tablename'] != tbl_curr:
        n = int(row['Number_Of_Records'])
        write_to_target('out_files/' + db_curr, tbl_curr, tbl_df)
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
            row['Nullable'] = 'N'
        data_dict = generate_key(row['Column'], row['Type'], val_min, val_max, static_value_list, len_val, precision_val, row['Nullable'], n)
        if row['Key'] == 'Primary Key':             
            pk_dict[row['Index']] =  list(data_dict.values())[0] 
        if row['Index'] in lookup_dict:   
            lookup_dict[row['Index']] = list(data_dict.values())[0]
        data_df = pd.DataFrame.from_dict(data_dict,orient='index').transpose()
        tbl_df[row['Column']] = data_df[row['Column']]
    
    elif row['Key'] == 'Foreign Key':
        if row['Dependency Index']:
            if row['Index'] > row['Dependency Index']: 
                if row['Dependency Index'] not in pk_dict:
                    print("Build the dependency before using it in the FK")
                    sys.exit(1)
                fk_list = [ random.choice(pk_dict[row['Dependency Index']]) for _ in range(n) ]
                if row['Index'] in lookup_dict:
                    lookup_dict[row['Index']] = fk_list
                tbl_df[row['Column']] = pd.Series(fk_list)

        else:
            print("For a foreign key, please specify dependency")
            sys.exit(1)
                
    elif row['Key'] == 'Composite Key': 
        if ck == True:
            print("Skipping a generated Composite Key Column")
            continue
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
        
        for c_ind, c_row in composite_cols_df.iterrows():
            if c_row['Index'] in lookup_dict: 
                lookup_dict[c_row['Index']] =  ck_df[str(c_row['Column'])].tolist() 
                            
        tbl_df = pd.concat([tbl_df,ck_df], axis=1)
        ck = True                 
    
    elif row['Key'] == 'Lookup':
        #print(lookup_dict)
        if not row['Dependency Index']:
            print("Please specify the column to lookup for Lookups")
            sys.exit(1)
            
        if row['Dependency Index'] not in lookup_dict:
            print("Lookup data not genertared yet")
            sys.exit(1)
        else:
            print("Generating Lookup data")
            lkp_list = [ random.choice(lookup_dict[row['Dependency Index']]) for _ in range(n) ]
            if row['Index'] in lookup_dict: 
                lookup_dict[row['Index']] = lkp_list 
            tbl_df[row['Column']] = pd.Series(lkp_list)
    
    elif row['Key'] == 'Unique Lookup':
        if not row['Dependency Index']:
            print("Please specify the column to lookup for Lookups")
            sys.exit(1)
            
        if row['Dependency Index'] not in lookup_dict:
            print("Lookup data not genertared yet")
            sys.exit(1)
        else:
            print("Generating Lookup data")
            lkp_set = set()
            for r in lookup_dict[row['Dependency Index']]:
                lkp_set.add(r)
                
                if len(lkp_set) > n-1:
                    break
                
            if len(lkp_set) < n:
                print("Not enough lookup values to generate unique lookup")
                sys.exit(1)
                
            if row['Index'] in lookup_dict: 
                lookup_dict[row['Index']] = list(lkp_set) 
                
            tbl_df[row['Column']] = pd.Series(list(lkp_set))
    
    else:
        data_dict = generate_attr(row['Column'], row['Type'], val_min, val_max, static_value_list, len_val, precision_val, row['Nullable'], n)
        data_df = pd.DataFrame.from_dict(data_dict,orient='index').transpose()
        if row['Index'] in lookup_dict: 
            lookup_dict[row['Index']] = data_dict.items()
        tbl_df[row['Column']] = data_df[row['Column']]

write_to_target('out_files/' + db_curr, tbl_curr, tbl_df)