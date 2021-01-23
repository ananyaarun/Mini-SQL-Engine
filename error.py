import csv
import sys
import sqlparse

def show_error(type):
    if type == 1:
	    print("Invalid! Semicolon missing.")
	    exit(0)
    elif type == 2:
	    print("Invalid! Query syntax incorrect.")
	    exit(0)
    elif type == 3:
	    print("Sorry! Database/Table does not exist.")
	    exit(0)
    elif type == 4:
	    print("Invalid! group by Query .")
	    exit(0)
    elif type == 5:
	    print("Invalid! Query syntax incorrect.")
	    exit(0)
    return