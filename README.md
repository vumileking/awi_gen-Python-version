# awi_gen-Python-version

This program is the first version of awi_gen data refining program. It checks if the data has errors and if not 
it sends it to the database. All the data is collected from redcap. 

If it happens that the data collected contains some errors, the program stores the partiants  ID in the database.
If the data with errors is updated on redcap, the program can collect the partiant ID from the table containing partiant ID
with errors and retrieve it from redcap. The table that contains errors has error discreption about the data.
This program also contains some flexible query, meaning you can analyse data stored on the database using simple statements.
