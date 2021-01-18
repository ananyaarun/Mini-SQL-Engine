import csv
import sys
import sqlparse

def show_error(type):
    if type == 1:
	    print("Invalid! Semicolon missing.")
	    exit(0)
    return