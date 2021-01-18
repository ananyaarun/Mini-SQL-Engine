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
distinct = 0

def main():
    create_db()
    query = sys.argv[1]
    
    query = sqlparse.format(query, keyword_case = 'upper')
    
    if ";" not in query:
    	show_error(1)

    if "SELECT" in query:
    	if "FROM" not in query:
    		show_error(1)
    else:
    	show_error(1)

    query = query.strip(";")
    
    q1 = query

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
        q4 = query
        group_q = ""

    if "WHERE" in q4:
	    q5 = re.split('WHERE',q4)
	    q6 = q5[0].strip(' ')
	    where_q = q5[1].strip(' ')

    else:
	    q6 = query
	    where_q = ""


    q7 = re.split('FROM',q6)[0].strip(' ')
    select_q = re.split('SELECT',q7)[1].strip(' ')
    from_q = re.split('FROM',q6)[1].strip(' ')

    print(select_q)
    print(from_q)
    print(where_q)
    print(group_q)
    print(order_q)






    

if __name__ == '__main__':
    main()