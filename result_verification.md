
#**Purpose**

This program takes results summary files exported from the election anomaly system and presents discrepancies between the two files. 

The input files should have the same `Top Reporting Unit` (e.g., Colorado) and the same `Sub Reporting Unit Type` (e.g., county).The program checks for discrepancies in count totals across both the files and gives an option to export these differences to a .csv file. It also flags any line items that are present in one file but missing in another one. 


#**Running the verification program**

Run *python src\election_anomaly\verify_results\__init__.py*

