#!/usr/bin/python2.7
#way of connecting
from redcap import Project, RedcapError
import psycopg2
TOKEN="token"
URL="https://redcap.core.wits.ac.za/redcap/redcap_v7.6.7/API/"
#connecting to database using postgreSQL
hostname='hostname'
username='username'
password='password'
database='APIPractice'
submit_data= []
to_database=[]
#making a connection link to the red cap
#response=Project(URL,TOKEN)
#connecting to database
#myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
#cursor=myConnection.cursor() 
#checkes if there are values that have been updated in study_error table
#err	
myConnection = psycopg2.connect( host=hostname, user=username, password=password,  dbname=database )
cursor=myConnection.cursor()
cursor.execute("SELECT awi_number FROM study_with_error;")
error_data=cursor.fetchall()
#compering study number from both tables
if error_data !="":
 for i in error_data:
  non_blank=0
  blank=0
  cpy= str(i)
  cursor.execute("DELETE FROM study_with_error WHERE awi_number='"+cpy[2:-3]+"'")
  myConnection.commit()
 # making connection with redcap
  response= Project(URL,TOKEN)
  records=[cpy[2:-3]]
 #extacting data
  data=response.export_records(records=records, format ='csv') 
  n=data.index('reimbursement_yesno')
  to_database=data[n+20: ]
  v=n
  j=data.index('table')
#\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
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
   cursor.execute("INSERT INTO study_with_error VALUES ('"+cpy[2:-3]+"','"+symbolise[0:50]+"','"+str(non_blank)+"','"+str(blank)+"');")
   myConnection.commit()
   print "Oups data still has error"
   pass
   myConnection = psycopg2.connect( host=hostname, user=username,   password=password,  dbname=database )
   cursor=myConnection.cursor()
#execute the function age_restriction if there is age above 20 and 80
  cursor.execute("SELECT age_restriction();")
  myConnection.commit()
  print" Done43"
  #executing function body_mass_index if there is abnormal BMI
  cursor.execute("SELECT body_mass_index();")
  myConnection.commit()
  submit_data=[]
  mass_index=0
  print" Done43"
  #calculating min and minimum values in ... field
  #cursor.execute("SELECT min_max();")
  cursor.execute("SELECT woman_pregnent_more_than_20_times();")
  myConnection.commit()
  print" Done43"
