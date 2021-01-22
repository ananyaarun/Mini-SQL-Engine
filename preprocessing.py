import csv
import sys
import sqlparse

tables_data={}
tables_info={}

def create_db():

    f = open("files/metadata.txt",'r')
    lines = f.readlines()
    f.close()
    temp = []
    ptr = 0
    chk = 0
    
    while ptr < len(lines):
    	temp = []
    	if lines[ptr][:13] == "<begin_table>":
    		ptr = ptr + 1
    		chk = 1

    	while lines[ptr][:11] != "<end_table>":
    		if chk == 1:
    			tab_name = lines[ptr].strip()
    			chk = 0
    		else:
    		    # print(lines[ptr].strip())
    		    temp.append(lines[ptr].strip())
    		ptr+=1

    	ptr = ptr + 1
    	tables_info[tab_name] = temp



    for tables in tables_info.keys():
    	f = open("files/" + tables + ".csv","r")
    	data = f.readlines()
    	f.close()

    	temp = []
    	for row in data:
    		vals = row.strip()
    		vals = vals.split(",")

    		for i in range(len(vals)):
    			vals[i] = vals[i].strip()
    			vals[i] = vals[i].replace('\'','')
    			vals[i] = vals[i].replace('\"','')
    			vals[i] = int(vals[i].strip())
    		
    		temp.append(vals)
    	tables_data[tables] = temp


    # print(tables_data)
    # print(tables_info)
