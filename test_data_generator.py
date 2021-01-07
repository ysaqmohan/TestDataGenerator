from faker import Faker
import numpy as np
import pandas as pd
import random
import sys 
import string

def derive_meta(min_field,max_field,static_field,length_field, precision_field,  n):
    '''Setting up the missing data in the input'''
    if min_field != min_field:
        val_min = 0
    else:
        val_min = int(min_field)
        
    if max_field != max_field:
        if length_field != length_field: 
            val_max = val_min + n*n
        else: 
            val_max = int(0.99999999999999 * (10**(int(length_field))))
    else:
        val_max = int(max_field)
            
    if static_field != static_field: 
        static_value_list = []
    else: 
        static_value_list = [rowval.strip() for rowval in static_field.split(',')]
        
    if precision_field != precision_field:
        precision_field = 2
    else:
        precision_field = int(precision_field)
        
    if length_field != length_field:
        length_field = 10
    else:
        length_field = int(length_field)
    
    return val_min, val_max, static_value_list, precision_field, length_field
    

def generate_key(column_name, type_of_rec, start_range, end_range, static_values, len_attr, number_of_rows):
    '''Generate values for records mentioned as primary key'''
    rec = []
    if static_values:
        if number_of_rows > len(static_values):
            for i in range(0,number_of_rows):
                if type_of_rec == 'country prefix': 
                    rec.append(faker.country_code())
                elif type_of_rec == 'country':
                    rec.append(faker.country())
                elif type_of_rec == 'phone number':
                    rec.append(faker.phone_number())
        else:
            for i in range(0,number_of_rows):
                rec.append(static_values[i])
        
    else:
        for i in range(0,number_of_rows):
            if type_of_rec == 'number': 
                rec.append(faker.unique.random_int(start_range, end_range))
            elif type_of_rec == 'ID':
                rec.append(''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(len_attr)))
            elif type_of_rec == 'country prefix': 
                    rec.append(faker.country_code())
                
    return({column_name: rec}) 

def generate_attr(column_name, type_of_rec, start_range, end_range, length_attr, static_values, precision_dec, number_of_rows):
    '''Generate values for records which are not PK or FK'''
    rec = []
    #print(column_name, type_of_rec, start_range, end_range, length_attr, static_values, number_of_rows)
    if not static_values: 
        for i in range(0,number_of_rows): 
            if type_of_rec == 'country prefix': 
                rec.append(faker.country_code()) 
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
                    sys.exit(1)
                
                if type_of_rec == 'char':
                    rec.append(faker.word())
                else:
                    rec.append(faker.word())
                    
    else: 
        for i in range(0,number_of_rows): 
            rec.append(random.choice(static_values))
                
    return({column_name: rec})             
           

user_ip_df = pd.read_excel('E:/TestDataGeneration/test_data_generation.xlsx', dtype=str)
faker = Faker(seed=1000)
tbl_df = pd.DataFrame()
for i, row in user_ip_df.iterrows():
        
    if i==0: #set up the first database and table
        db_curr = row['Databasename'] 
        tbl_curr = row['Tablename']
        n = int(row['Number_Of_Records'])            
    
    if row['Databasename'] != db_curr or row['Tablename'] != tbl_curr:
        n = int(row['Number_Of_Records'])
        tblename = db_curr + '_' + tbl_curr + '.csv'
        tbl_df.to_csv(tblename)
        tbl_df = pd.DataFrame()
        db_curr = row['Databasename'] 
        tbl_curr = row['Tablename']
        
    val_min, val_max, static_value_list, precision_val, len_val =  derive_meta(row['Minimum'], row['Maximum'], row['Static_Value'], row['Length'], row['Precision'], n)
       
    if row['Key'] == 'Primary Key' or row['Key'] == 'Unique':        
        data_dict = generate_key(row['Column'], row['Type'], val_min, val_max, static_value_list, len_val, n)
        data_df = pd.DataFrame.from_dict(data_dict,orient='index').transpose()
        tbl_df[row['Column']] = data_df[row['Column']]
    
    elif row['Key'] == 'Foreign Key':
        tbl_df[row['Column']] = np.nan
    
    else:
        data_dict = generate_attr(row['Column'], row['Type'], val_min, val_max, row['Length'], static_value_list, precision_val, n)
        data_df = pd.DataFrame.from_dict(data_dict,orient='index').transpose()
        tbl_df[row['Column']] = data_df[row['Column']]

tblename = db_curr + '_' + tbl_curr + '.csv'        
tbl_df.to_csv(tblename)    
    

        
    
