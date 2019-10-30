#!/usr/bin/env python
# coding: utf-8

# In[1]:


import csv
from datetime import datetime, timedelta
import pyodbc


# In[2]:


conn = pyodbc.connect('DSN=kubricksql;UID=de14;PWD=password')
cur = conn.cursor()


# In[3]:


sharkfile = r'C:\data\GSAF5.csv'


# In[4]:


attack_dates = []
case_number = []
country = []
activity = []
age = []
gender = []
is_fatal = []
with open(sharkfile) as f:
    reader = csv.DictReader(f)
    for row in reader: #each row is a dictionary (remember reader is a list of dictionaries)
        #print(row)
        attack_dates.append(row['Date'])
        case_number.append(row['Case Number'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        is_fatal.append(row['Fatal (Y/N)'])


# In[5]:


data = zip(attack_dates, case_number, country, activity, age, gender, is_fatal)


# In[6]:


cur.execute('truncate table simonwalker.shark')


# In[7]:


q = 'INSERT INTO simonwalker.shark ([attack_date], [case_number], [country], [activity], [age], [gender], [is_fatal]) VALUES (?, ?, ?, ?, ?, ?, ?)'


# In[8]:


for d in data:
    try:
        cur.execute(q,d)
        conn.commit()
    except:
        conn.rollback


# In[ ]:


# then go to file and export notebook as executable script

