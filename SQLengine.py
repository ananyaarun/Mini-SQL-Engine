import csv
import sys
import sqlparse
import re
from preprocessing import *
from error import *

tabs = []
select_q = ""
from_q = ""
where_q = ""
group_q = ""
order_q = ""
dis_flag = 0
join_table = {}
oper = {3:"<", 4:">", 1:"<=", 2:">=", 5:"="}


def get_oper_type(query):

    if "<=" in query: 
        opertype = 1
    elif ">=" in query:
        opertype = 2
    elif "<" in query:
        opertype = 3
    elif ">" in query:
        opertype = 4
    elif "=" in query:
        opertype = 5
    return opertype


def eval_without(data,col,val,op):
    
    col_index = 0
    rows_to_remove = []
    for i in data[0]:
        temp = re.split('.',i)[1].strip(' ')
        if str(temp) == str(col):
            
    return


def eval_and(data,col1,val1,op1,col2,val2,op2):
    print(col1)
    print(val1)
    print(col2)
    print(val2)
    return


def eval_or(data,col1,val1,op1,col2,val2,op2):
    print(col1)
    print(val1)
    print(col2)
    print(val2)
    return


def join_tabs(tab1, tab2):

    a = tab1[0]
    b = tab2[0]
    ret_tab = [a + b]
    tot1 = len(tab1)
    tot2 = len(tab2)

    for i in range(1,tot1):
        row1 = tab1[i]
        for j in range(1, tot2):
            row2 = tab2[j]
            temp = row1 + row2
            ret_tab.append(temp)
    return ret_tab



def join_all(Tnames):

    data = []
    for name in Tnames:
        col_names = [name + '.' + s for s in tables_info[name]]
        tmp_table = [col_names] + tables_data[name]
        if data == []:
            data = tmp_table
        else:
            data = join_tabs(data, tmp_table)
    return data


def process_where(data,query):
    flag = 0
    opertype = 0
    
    if "AND" in query:
        flag = 1
    if "OR" in query:
        flag = 2

    if flag == 0:
        opertype = get_oper_type(query)
        temp = re.split(oper[opertype],query)
        col = temp[0].strip(' ')
        val = temp[1].strip(' ')
        eval_without(data,col,val,opertype)

    elif flag == 1:

        temp = re.split("AND",query)
        q1 = temp[0].strip(' ')
        q2 = temp[1].strip(' ')
        opertype1 = get_oper_type(q1)
        opertype2 = get_oper_type(q2)

        temp = re.split(oper[opertype1],q1)
        col1 = temp[0].strip(' ')
        val1 = temp[1].strip(' ')

        temp = re.split(oper[opertype2],q2)
        col2 = temp[0].strip(' ')
        val2 = temp[1].strip(' ')

        eval_and(data,col1,val1,opertype1,col2,val2,opertype2)

    elif flag == 2:

        temp = re.split("OR",query)
        q1 = temp[0].strip(' ')
        q2 = temp[1].strip(' ')
        opertype1 = get_oper_type(q1)
        opertype2 = get_oper_type(q2)

        temp = re.split(oper[opertype1],q1)
        col1 = temp[0].strip(' ')
        val1 = temp[1].strip(' ')

        temp = re.split(oper[opertype2],q2)
        col2 = temp[0].strip(' ')
        val2 = temp[1].strip(' ')

        eval_or(data,col1,val1,opertype1,col2,val2,opertype2)
    

def main():
    create_db()
    query = sys.argv[1]
    
    query = sqlparse.format(query, keyword_case = 'upper')
    
    if ";" not in query:
    	show_error(1)

    if "SELECT" in query:
    	if "FROM" not in query:
    		show_error(2)
    else:
    	show_error(2)

    query = query.strip(";")
    
    q1 = query
    q6 = query
    where_q = ""

    if "DISTINCT" in query:
        dis_flag = 1

    if "ORDER BY" in query:
	    q1 = re.split('ORDER BY',query)
	    q2 = q1[0].strip(' ')
	    order_q = q1[1].strip(' ')
    else:
	    q2 = query
	    order_q = ""

    if "GROUP BY" in q2:
	    q3 = re.split('GROUP BY',q2)
	    q4 = q3[0].strip(' ')
	    group_q = q3[1].strip(' ')

    else:
        if "ORDER BY" in query:
            q4 = q2
        else:
            q4 = query
        group_q = ""

    if "WHERE" in q4:
	    q5 = re.split('WHERE',q4)
	    q6 = q5[0].strip(' ')
	    where_q = q5[1].strip(' ')
    else:
	    if "ORDER BY" in query:
		    q6 = q2
	    if "GROUP BY" in query:
		    q6 = q4

    q7 = re.split('FROM',q6)[0].strip(' ')
    select_q = re.split('SELECT',q7)[1].strip(' ')
    from_q = re.split('FROM',q6)[1].strip(' ')

    print("Debugggginggg")
    print(select_q)
    print(from_q)
    print(where_q)
    print(group_q)
    print(order_q)

    # processing from - finding join 
    Tnames = [T.strip(' ') for T in from_q.split(',')]
    data = join_all(Tnames)
    print(data)
    # order to evaluate in - from where groupby select distinct orderby
    if where_q != "":
        process_where(data,where_q)
    
   

if __name__ == '__main__':
    main()