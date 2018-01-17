# awi_gen-Python-version

The program is developed using Python programming language. Data used by the this program is downloaded from redcap and inserted to PostgreSQL after it has been refined and validated.

  # recapTest.py step one.
   
It allows the user to type in and specify the range of data  user want to download from redcap, It only requires patient id then it gets all the content requested. It download one data content at the time.
After downloading the data there are two ways to refine the data. The first way is to check if the data consists of some data semantic errors, for example suppose the we expect the column name to have string of characters not numbers, so if it has numbers this generates an error.

The second error validation process is found in the PostgreSQL in the database. In the database I wrote a code that checks for age restriction, body mass index, woman who got more than twenty babies and man who are pregnant.

In the database we have two tables. The first table is awi_gen_api_practiceiii which consists of refined data, this table is the mostly table for data analysis. The second table is study_with_error, which consists patient id and error description about the error found in the data.

There is an additional flexible maximum and minimum function, allowing the user to check the minimum and maximum  values of a specified column.

 # recapTest.py step two

Since the data that consists of errors is continuously corrected the program need to constantly update. Everytime the program is run, after doing step one it collects all the patient id from table study_with_error which consists of data with errors then re-download it from redcap. Until it is done with the patient id in study_with_error table 
