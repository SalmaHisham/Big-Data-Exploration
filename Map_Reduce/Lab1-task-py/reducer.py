import sys

# first existance
current_city = None
total_temp = 0
temp_cnt = 0

for line in sys.stdin:
  city, c_temp = line.split(',')
  c_temp = float(c_temp)
  if current_city == city:
    total_temp += c_temp
    temp_cnt += 1
  else:
    if current_city:
        temp_avg = total_temp / temp_cnt
        print(f'{current_city}, {temp_avg}')

    current_city = city
    total_temp = c_temp
    temp_cnt = 1
