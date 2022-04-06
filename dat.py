from datetime import datetime, timedelta, date, time
import calendar
import datetime as dt
from pendulum import time

class Reporting_variables:
    def __init__(self):
        pass
    def yesterday(self):
        yesterday = datetime.utcnow() - timedelta(1)
        yesterday = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return yesterday

def last_day_of_month(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - timedelta(days=next_month.day)

def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return datetime(year, month, day)

def diff_month(d1, d2):
    return (d1.year - d2.year) * 12 + d1.month - d2.month

test_date = ['2022-01-01']
first_date_this_month = date.today().replace(day=1)
first_date_last_month = add_months(first_date_this_month, -1).replace(day=1).date()

latest_month_dt = datetime.strptime(test_date[0], '%Y-%m-%d').replace(day=1)
latest_month = latest_month_dt.date()

month_diff = diff_month(first_date_last_month, latest_month)


month_ls = []
if month_diff == 0:
    month_ls.append(latest_month)
else: 
    for k in range(0, month_diff+1):
        month_to_add = add_months(latest_month, k).replace(day=1)
        month_ls.append(month_to_add)
            
# last_date_last_month = last_day_of_month(latest_month).replace(hour=16)

# last_date_current_month= last_day_of_month(add_months(latest_month, 1)).replace(hour=16)
print(latest_month,first_date_last_month, month_ls)

for dd in month_ls:
    testtime = datetime.combine(dd, time(0,0,0))
    print(testtime)



default_this_month = date.today().replace(day=1)
default_threeback_month = add_months(default_this_month, -4).replace(day=1).date()

default_condition_date = str(default_threeback_month)

print(default_threeback_month, default_condition_date)



