# !/opt/tljh/user/bin/python
import sys
# input comes from standard input
for line in sys.stdin:
    tokens = line.strip().split(',')
    city = tokens[0]
    f_temp = tokens[1]
    try:
        c_temp = (float(f_temp) - 32) * 5.0/9.0
    except ValueError:
        # ignore if count was not a number
        continue
    print(f"{city}, {c_temp}")
