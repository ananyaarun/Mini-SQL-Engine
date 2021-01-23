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
agg = {3:"sum", 4:"avg", 1:"min", 2:"max", 5:"count"}


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


def test_cond(row_val, op, exp_val):
    if op == 1:
        if row_val <= int(exp_val):
            return True
        else:
            return False
    elif op == 2:
        if row_val >= int(exp_val):
            return True
        else:
            return False
    elif op == 3:
        if row_val < int(exp_val):
            return True
        else:
            return False
    elif op == 4:
        if row_val > int(exp_val):
            return True
        else:
            return False
    elif op == 5:
        if row_val == int(exp_val):
            return True
        else:
            return False



def eval_without(data,col,val,op):
    col_index = -1
    col_if = 0
    ind = 0
    chk = 0
    for i in data[0]:
        temp = i.split('.')[1]
        if str(temp) == str(col):
            col_index = ind
        if str(temp) == str(val):
            col_if = ind
            chk = 1
        ind += 1

    if col_index == -1:
        show_error(3)
    if chk == 0:
        data1 = [v for v in data[1:] if test_cond(v[col_index],op,val)]
    elif chk == 1:
        data1 = [v for v in data[1:] if test_cond(v[col_index],op,v[col_if])]
    
    final = []
    final.append(data[0])
    for i in data1:
        final.append(i)

    return final


def eval_and(data,col1,val1,op1,col2,val2,op2):
    
    col_index1 = -1
    col_index2 = -1
    col_if1 = 0
    col_if2 = 0
    ind = 0
    chk1 = 0
    chk2 = 0

    for i in data[0]:
        temp = i.split('.')[1]
        if str(temp) == str(col1):
            col_index1 = ind
        if str(temp) == str(col2):
            col_index2 = ind
        if str(temp) == str(val1):
            col_if1 = ind
            chk1 = 1
        if str(temp) == str(val2):
            col_if2 = ind
            chk2 = 1
        ind += 1

    if col_index1 == -1 or col_index2 == -1:
        show_error(3)
    if chk1 ==0 and chk2 == 0:
        data1 = [v for v in data[1:] if test_cond(v[col_index1],op1,val1) and test_cond(v[col_index2],op2,val2)]
    elif chk1 ==0 and chk2 == 1:
        data1 = [v for v in data[1:] if test_cond(v[col_index1],op1,val1) and test_cond(v[col_index2],op2,v[col_if2])]
    elif chk1 ==1 and chk2 == 0:
        data1 = [v for v in data[1:] if test_cond(v[col_index1],op1,v[col_if1]) and test_cond(v[col_index2],op2,val2)]
    elif chk1 ==1 and chk2 == 1:
        data1 = [v for v in data[1:] if test_cond(v[col_index1],op1,v[col_if1]) and test_cond(v[col_index2],op2,v[col_if2])]
    
    final = []
    final.append(data[0])
    for i in data1:
        final.append(i)

    return final


def eval_or(data,col1,val1,op1,col2,val2,op2):
    
    col_index1 = -1
    col_index2 = -1
    col_if1 = 0
    col_if2 = 0
    ind = 0
    chk1 = 0
    chk2 = 0

    for i in data[0]:
        temp = i.split('.')[1]
        if str(temp) == str(col1):
            col_index1 = ind
        if str(temp) == str(col2):
            col_index2 = ind
        if str(temp) == str(val1):
            col_if1 = ind
            chk1 = 1
        if str(temp) == str(val2):
            col_if2 = ind
            chk2 = 1
        ind += 1

    if col_index1 == -1 or col_index2 == -1:
        show_error(3)
    if chk1 ==0 and chk2 == 0:
        data1 = [v for v in data[1:] if test_cond(v[col_index1],op1,val1) or test_cond(v[col_index2],op2,val2)]
    elif chk1 ==0 and chk2 == 1:
        data1 = [v for v in data[1:] if test_cond(v[col_index1],op1,val1) or test_cond(v[col_index2],op2,v[col_if2])]
    elif chk1 ==1 and chk2 == 0:
        data1 = [v for v in data[1:] if test_cond(v[col_index1],op1,v[col_if1]) or test_cond(v[col_index2],op2,val2)]
    elif chk1 ==1 and chk2 == 1:
        data1 = [v for v in data[1:] if test_cond(v[col_index1],op1,v[col_if1]) or test_cond(v[col_index2],op2,v[col_if2])]
    
    final = []
    final.append(data[0])
    for i in data1:
        final.append(i)

    return final


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
        if name not in tables_info.keys():
            show_error(3)
        col_names = [name + '.' + s for s in tables_info[name]]
        tmp_table = [col_names] + tables_data[name]
        if data == []:
            data = tmp_table
        else:
            data = join_tabs(data, tmp_table)
    return data


def func_agg_groups(arr,tp):
    # print(arr)
    # print(tp)
    if tp == 1:
        return sum(arr)
    elif tp == 2:
        return sum(arr)/len(arr)
    elif tp == 3:
    	return min(arr)
    elif tp == 4:
    	return max(arr)
    elif tp == 5:
    	return len(arr)
    else:
    	return arr
    

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
        data = eval_without(data,col,val,opertype)

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

        data = eval_and(data,col1,val1,opertype1,col2,val2,opertype2)

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

        data = eval_or(data,col1,val1,opertype1,col2,val2,opertype2)

    return data

def get_agg_type(orig):
    agg_type = 0
    if "sum" in orig: 
        agg_type = 1
    if "avg" in orig:
        agg_type = 2
    if "min" in orig: 
        agg_type = 3
    if "max" in orig: 
        agg_type = 4
    if "count" in orig:
        agg_type = 5
    # print(agg_type)
    return agg_type

def modify_func(data,orig):
    new_data = []
    for i in data:
        nam = i.split('.')
        ini = nam[0]
        nam = nam[1]
        if "sum("+str(nam)+")" in orig or "SUM("+str(nam)+")" in orig:
            new_data.append(str(ini)+".sum("+str(nam)+")")
        elif "avg("+str(nam)+")" in orig or "AVG("+str(nam)+")" in orig:
            new_data.append(str(ini)+".avg("+str(nam)+")")
        elif "min("+str(nam)+")" in orig or "MIN("+str(nam)+")" in orig:
            new_data.append(str(ini)+".min("+str(nam)+")")
        elif "max("+str(nam)+")" in orig or "MAX("+str(nam)+")" in orig:
            new_data.append(str(ini)+".max("+str(nam)+")")
        elif "count("+str(nam)+")" in orig or "COUNT("+str(nam)+")" in orig:
            new_data.append(str(ini)+".count("+str(nam)+")")
        else:
            new_data.append(i) 
    
    return new_data


def process_group (data, query,orig):

    col_index = 0
    ind = 0
    for i in data[0]:
        temp = i.split('.')[1]
        if str(temp) == str(query):
            col_index = ind
        ind += 1

    list_uni = []
    for i in data[1:]:
        ind = 0
        for j in i:
            if ind == col_index and j not in list_uni:
                list_uni.append(j)
            ind += 1
    
    new_dat = []
    new_dat.append(modify_func(data[0],orig))
    # print(new_dat)
    no_cols = len(data[0]);
    grouped_data = []
    agg_type = 0

    for i in list_uni:
        temp = []
        temp1 = {}
        for j in range(0,no_cols):
            if j == col_index:
            	continue
            for k in range(0,len(data)-1):
                if data[k+1][col_index] == i:
                    if j not in temp1.keys():
                        temp1[j]=[]
                    temp1[j].append(data[k+1][j])
                    if temp1 not in temp:
                        temp.append(temp1) 
        grouped_data.append(temp)

    fin_gp_data = []

    ind = 0
    for j in grouped_data:
        grps = []
        grps.append(list_uni[ind])
        ind+=1
        for k in j:
       	    for l in k.keys():
       	        grps.append(func_agg_groups(k[l],get_agg_type(new_dat[0][l])))
       	fin_gp_data.append(grps)

    for i in fin_gp_data:
        new_dat.append(i)
    return new_dat
  

def process_order(data,query):

    if "ASEC" in query or "asec" in query or "DESC" in query or "desc" in query:
        temp = query.split(' ')
        col_name = temp[0]
        typ = temp[1]
    else:
    	typ = "ASEC"
    	col_name = query

    col_index = 0
    ind = 0
    for i in data[0]:
        temp = i.split('.')[1]
        if str(temp) == str(col_name):
            col_index = ind
        ind += 1
    
    nndata=[]
    nndata.append(data[0])
    
    if typ == "DESC" or typ == "desc":
        ndata = sorted(data[1:], key=lambda x : x[col_index],reverse = True)
    else:
        ndata = sorted(data[1:], key=lambda x : x[col_index])

    for i in ndata:
        nndata.append(i)
    return nndata


def process_select_new(data,query):
    if query == '*':
        return data

    aa = ["sum","SUM","avg","AVG","min","MIN","max","MAX","count","COUNT"]

    cols = query.strip(' ').split(',')
    col = []
    
    chkflg = 0
    anan = 0
    for i in cols:
        # print(str(anan) + " " +str(i)+ " " + str(chkflg))
        if anan>0 and chkflg==0 and '(' in i:
            show_error(2)
        if '(' in i:
            chkflg = 1
        if chkflg == 1 and '(' not in i:
            show_error(2)
        anan+=1

    if chkflg == 1:
        ans = []
        for i in cols:
	        at = 0
	        wordd = i.split('(')
	        temp = wordd[1].split(')')[0]
	        wordd = wordd[0]
	        if wordd.lower() == "sum":
	            at = 1
	        elif wordd.lower() == "avg":
	            at = 2
	        elif wordd.lower() == "min":
	            at = 3
	        elif wordd.lower() == "max":
	            at = 4
	        elif wordd.lower() == "count":
	            at = 5

	        ind = 0
	        for i in data[0]:
		        tempp = i.split('.')[1]
		        if str(tempp) == str(temp):
		            col_ind = ind
		        ind += 1
	        
	        new_data=[]
	        new_data.append(str(wordd.lower())+"("+str(temp)+")")
	        arr = [row[col_ind] for row in data[1:]]
	        new_data.append(func_agg_groups(arr,at))
	        ans.append(new_data)
        return ans

    if chkflg == 0:
	    for i in cols:
	        i=i.strip(' ')
	        if '(' in i:
	            temp = i.split('(')
	            col.append(str(temp[0].lower())+"("+str(temp[1]))
	        else:
	            col.append(i)

	    # print(col)
	    col_index = []
	    col_ind = -1
	    for j in col:
	        ind = 0
	        for i in data[0]:
	            temp = i.split('.')[1]
	            if str(temp) == str(j):
	                col_ind = ind
	            ind += 1
	        if col_ind != -1:
	            col_index.append(col_ind)


	    if col_ind==-1:
	        show_error(3)
        

        
	    
	    fin=[]
	    
	    for i in col_index:
	        fin.append([row[i] for row in data])

	    finn = []
	    finnn = []

	    for i in range(0,len(fin[0])):
	        finn = []
	        for j in range(0,len(fin)):
	            finn.append(fin[j][i])
	        finnn.append(finn)
	    return finnn




def process_select(data, query):
    if query == '*':
        return data

    cols = query.strip(' ').split(',')
    col = []
    for i in cols:
        i=i.strip(' ')
        if '(' in i:
            temp = i.split('(')
            col.append(str(temp[0].lower())+"("+str(temp[1]))
        else:
            col.append(i)

    # print(col)
    col_index = []
    col_ind = -1
    for j in col:
        ind = 0
        for i in data[0]:
            temp = i.split('.')[1]
            if str(temp) == str(j):
                col_ind = ind
            ind += 1
        if col_ind != -1 :
            col_index.append(col_ind)

    fin=[]
    if col_ind==-1:
        show_error(3)
    
    for i in col_index:
        fin.append([row[i] for row in data])

    finn = []
    finnn = []

    for i in range(0,len(fin[0])):
        finn = []
        for j in range(0,len(fin)):
            finn.append(fin[j][i])
        finnn.append(finn)
    return finnn

def main():
    create_db()
    query = sys.argv[1]
    # print(query)
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

    # processing from - finding join 
    Tnames = [T.strip(' ') for T in from_q.split(',')]
    data = join_all(Tnames)
   
    # print(data)
    
    # order to evaluate in - from where groupby select distinct orderby
   
    if where_q != "":
        data = process_where(data,where_q)

    if group_q != "":
        data = process_group(data,group_q,query)
        # print(data)
    if select_q != "":
        # print(select_q)

        if group_q == "":
            dis_flag = 0        
            if "DISTINCT" in select_q:
                dis_flag = 1
                select_q = select_q.replace('DISTINCT','').strip(' ')
            data = process_select_new(data,select_q)
            if dis_flag == 1:
                temp = []
                for i in data:
                    if i not in temp:
                        temp.append(i)
                data = temp

        else:
            dis_flag = 0        
            if "DISTINCT" in select_q:
                dis_flag = 1
                select_q = select_q.replace('DISTINCT','').strip(' ')
            data = process_select(data,select_q)
            if dis_flag == 1:
                temp = []
                for i in data:
                    if i not in temp:
                        temp.append(i)
                data = temp
    
    if order_q != "":
        data = process_order(data,order_q)


    # print(data)
    
    for i in range(0,len(data)):
        for j in range(0,len(data[i])):
            if j == len(data[i])-1:
                print(data[i][j],end=" ")
            else:
                print(data[i][j], end=",")
        print('')

if __name__ == '__main__':
    main()