import csv
import sys
import sqlparse
from preprocessing import *

def main():
    create_db()
    query = sys.argv[1]
    basic_error_chk(query)
    parse_q(query)


if __name__ == '__main__':
    main()