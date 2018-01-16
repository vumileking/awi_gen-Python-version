#!/usr/bin/python
#way of connecting
from redcap import Project, RedcapError
import psycopg2
TOKEN="token"
URL="https://redcap.core.wits.ac.za/redcap/api/"
#connecting to database using postgreSQL
hostname='hostname'
username='username'
password='password'
database='APIPractice'
submit_data= []
to_database=[]
#making a connection link to the red cap
response=Project(URL,TOKEN)
#Getting data row by row using awi_number sincce it is our primary key
for y in range(272,272):
 non_blank=0
 blank=0
# starting with the first 10 awi_number
 if y<10:
  ID="P000"+str(y)
  print ID
  #making a connection link to the red cap
  response=Project(URL,TOKEN)
  records=[ID]
  #extracting data
  data=response.export_records(records=records, format='csv')
  n=data.index('reimbursement_yesno')
  to_database=data[n+20: ]
  v=n
  j=data.index('table')
   # print to_database
  #connecting to database
  myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
  cursor=myConnection.cursor()
  n=0
 #getting the data ready for insertion to the database
  for i in  str(to_database[:-1]):
     #print str(to_database[:-1])
   if i ==',' and n!=",":
    submit_data.append("','")
    non_blank+=1
      #print i
   elif i=='\n':
    submit_data.append("'),('")
    non_blank+=1
      #print i
   elif n=="," and i==',':
    submit_data.append("0','")
    blank+=1
   else:
    submit_data.append(i)
   n=i
      #print i
    #print ''.join(map(str,submit_data[:-1]))
#exercuting a query for database
  T=data[0:j+26]+"table_,"+data[j+32:v+19]
  columns=[]
  J=0 
  try:
   for i in T:
    if i==',':
     columns.append(', ')
     J+=1
    else:
      columns.append(i)
   print J
   cursor.execute("INSERT INTO  awi_gen_api_practiceiii VALUES ('"+''.join(map  (str,submit_data[:-1]))+"0');")
   print "done importing data to database"
   myConnection.commit()
   #results= cursor.fatchall()
     #myConnection.close()
    #IF THERE IS A DATA ERROR, RECORDS THE STUDY NUMBER WITH DATA ERROR
  except (Exception,psycopg2.DataError) or (Exception,psycopg2.ProgrammingError)  or (Exception,psycopg2.IntegrityError)as error:
   myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
   cursor=myConnection.cursor()
   symbolise= ''.join(map(str,error[0:50]))
   cursor.execute("INSERT INTO study_with_error VALUES ('"+ID+"','"+symbolise[0:50]+"','"+str(non_blank)+"','"+str(blank)+"');")
   print error
   myConnection.commit()
   pass
  submit_data=[]
  mass_index=0
 else:
  if y<100 and y>9:#
   ID="P00"+str(y)
   #extracting data
   print ID
   records=[ID]
   #data validation stage
 #making a connection link to the red cap
   response=Project(URL,TOKEN)
   #extracting data
   records=[ID]
   data=response.export_records(records=records, format='csv')
   n=data.index('reimbursement_yesno')
   to_database=data[n+20: ]
  #conntcing to database
   myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
   cursor=myConnection.cursor()
  #getting the data ready for insertion to the database
   for i in str(to_database[:-1]):
     #print str(to_database[:-1])
    if i ==',' and n!=",":
     submit_data.append("','")
     non_blank+=1
      #print i
    elif i=='\n':
     submit_data.append("'),('")
     non_blank+=1
      #print i
    elif n=="," and i==',':
     submit_data.append("0','")
     blank+=1
    else:
     submit_data.append(i)
    n=i
      #print i
  #exercuting a query for database 
   try:
    cursor.execute("INSERT INTO awi_gen_api_practiceiii VALUES ('"+''.join(map    (str,submit_data[:-1]))+"0');")
    myConnection.commit()
    print "importing data to database"
   #results= cursor.fatchall()
      #myConnection.close()
#IF THERE IS A DATA ERROR, RECORDS THE STUDY NUMBER WITH DATA ERROR
   except (Exception,psycopg2.DataError) or (Exception,psycopg2.ProgrammingError)  or (Exception,psycopg2.IntegrityError)as error:
    myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
    cursor=myConnection.cursor()
    symbolise= ''.join(map(str,error[0:50]))
    cursor.execute("INSERT INTO study_with_error VALUES ('"+ID+"','"+symbolise[0:50]+"','"+str(non_blank)+"','"+str(blank)+"');")
    myConnection.commit()
    print "data err, sending to study_with_err table"
    pass
   submit_data=[]
   mass_index=0
  else:
   if y>99 and y<1000:#
    ID="P0"+str(y)
    print ID
    #extracting data
    records=[ID]
     #data validation stage
    #making a connection link to the red cap
    response=Project(URL,TOKEN)
    #extracting data
    records=[ID]
    data=response.export_records(records=records, format='csv')
    n=data.index('reimbursement_yesno')
    to_database=data[n+20: ]
    #conntcing to database
    myConnection = psycopg2.connect( host=hostname, user=username,      password=password,  dbname=database )
    cursor=myConnection.cursor()
     #getting the data ready for insertion to the database
    for i in str(to_database[:-1]):
     #print str(to_database[:-1])
     if i ==',' and n!=",":
      submit_data.append("','")
      non_blank+=1
      #print i
     elif i=='\n':
      submit_data.append("'),('")
      non_blank+=1
      #print i
     elif n=="," and i==',':
      submit_data.append("0','")
      blank+=1
     else:
      submit_data.append(i)
     n=i
      #print i
      #exercuting a query for database 
    try:
     cursor.execute("INSERT INTO awi_gen_api_practiceiii VALUES ('"+''.join(map    (str,submit_data[:-1]))+"0');")
     print" data inserted to database"
     myConnection.commit()
     #results= cursor.fatchall()
       #myConnection.close()
    except (Exception,psycopg2.DataError) or (Exception,psycopg2.ProgrammingError)  or (Exception,psycopg2.IntegrityError)as error:
#IF THERE IS A DATA ERROR, RECORDS THE STUDY NUMBER WITH DATA ERROR
      myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
      cursor=myConnection.cursor()
      symbolise= ''.join(map(str,error[0:50]))
      cursor.execute("INSERT INTO study_with_error VALUES ('"+ID+"','"+symbolise[0:50]+"','"+str(non_blank)+"','"+str(blank)+"');")
      print "data err, sending to study_with_err table"
      pass
    submit_data=[]
    mass_index=-1
   else:
    if y>999 and 3000:#
     ID="P"+str(y)
     print "P1000 to P3000"
     #extracting data
     records=[ID]
     #making a connection link to the red cap
     response=Project(URL,TOKEN)
      #extracting data
     records=[ID]
     data=response.export_records(records=records, format='csv')
     n=data.index('reimbursement_yesno')
     to_database=data[n+20: ]
     #conntcing to database
     myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
     cursor=myConnection.cursor()
     #getting the data ready for insertion to the database
     for i in str(to_database[:-1]):
     #print str(to_database[:-1])
      if i ==',' and n!=",":
       submit_data.append("','")
       non_blank+=1
      #print i
      elif i=='\n':
       submit_data.append("'),('")
       non_blank+=1
      #print i
      elif n=="," and i==',':
       submit_data.append("0','")
       blank+=1
      else:
       submit_data.append(i)
      n=i
      #print i
    #exercuting a query for database 
     try:
      cursor.execute("INSERT INTO awi_gen_api_practiceiii VALUES ('"+''.join(map  (str,submit_data[:-1]))+"0');")
      print "data sent to database"
      myConnection.commit()
     #results= cursor.fatchall()
      print cursor
        #myConnection.close() 
     except (Exception,psycopg2.DataError) or (Exception,psycopg2.ProgrammingError)  or (Exception,psycopg2.IntegrityError)as error:
#IF THERE IS A DATA ERROR, RECORDS THE STUDY NUMBER WITH DATA ERROR
      myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
      cursor=myConnection.cursor()
      symbolise= ''.join(map(str,error[0:50]))
      cursor.execute("INSERT INTO study_with_error VALUES ('"+ID+"','"+symbolise[0:50]+"','"+str(non_blank)+"','"+str(blank)+"');")
      print "data err, sending to study_with_err table"
      myConnection.commit()
      pass   
     submit_data=[]
     mass_index=0
print "done"
data_id=[]
#checking is weight an hight make sence
myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
cursor=myConnection.cursor()
#execute the function age_restriction if there is age above 20 and 80
cursor.execute("SELECT age_restriction();")
myConnection.commit()
print" Done43"
#executing function body_mass_index if there is abnormal BMI
cursor.execute("SELECT body_mass_index();")
myConnection.commit()
print" Done43"
#calculating min and minimum values in ... field
#cursor.execute("SELECT min_max();")
cursor.execute("SELECT woman_pregnent_more_than_20_times();")
myConnection.commit()
print" Done43"
#dynamic function allows you to be check maximum, minimum and avarage of any column you search o database
responce=''
#if N means break from loop
while response!='N':
 print "Do you want to check Maximum, minimum and avarage of any column?"
 print' '
 response=raw_input('type Y- for yes and N-for no: ')
 if response=="Y":
#if the condition is yes, then the search loop for start
  print  " "
#establish database connection 
  myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
  cursor=myConnection.cursor()
#request for column name, NOTE: column name mast be the same as the name on database column.
  column= raw_input("Please insert the name of the column : ")
  print " "
#the where condition must be with accordance to the way sql grammar is.
#IF THE WHERE CONDITION IS NOT REQUERED THEN THE USER MUST PRESS ENTER 
  where_statment= raw_input('Please insert your (WHERE) condition, if requered.\n If not requered press Enter: ')
  if where_statment =="":
   print ""
# table name must be the same as the name on on database
   table= raw_input('Please insert your table name: ')
   print " "
#sql query with respect to the condintions you have inserted.
   cursor.execute("SELECT Max("+column+") AS max_colum, Min("+column+") AS min_column, AVG("+column+") FROM "+table+";")
#preper results from print out, with respect to the sql statement
   row=cursor.fetchall()
#check if the statement is not empty
   if row is not None:
    print" \n"
#the print statement starts with the maximun, followed by minimum and avarage
    print'_____________________________________________'
    print '(Maximum | Minimum| Avarage'
#converting the outcome as string
    print ''.join(map(str,row)) 
    print'_____________________________________________\n'
   else:
    print "Query not found.\n"
  else:
   print ""
# table name must be the same as the name on on database
   table= raw_input('Please insert your table name: ')
   print " "
#sql query with respect to the condintions you have inserted.
   cursor.execute("SELECT Max("+column+") AS max_colum, Min("+column+") AS min_column, AVG("+column+") FROM "+table+" WHERE "+where_statment+";")
#preper results from print out, with respect to the sql statement
   row=cursor.fetchall()
#check if the statement is not empty
   if row is not None:
    print" \n"
#the print statement starts with the maximun, followed by minimum and avarage
    print'_____________________________________________'
    print '(Maximum | Minimum| Avarage'
#converting the outcome as string
    print ''.join(map(str,row)) 
    print'_____________________________________________\n'
   else:
    print "Query not found.\n"
#closing connection
MyConnection.close()
