import datetime

_weeks_ago = 0

_start_date = (datetime.datetime.today() - datetime.timedelta(days=datetime.datetime.today().isoweekday() % 7 + 6 + (7 * _weeks_ago))).date()
_end_date = _start_date + datetime.timedelta(days=6)
print(_start_date.strftime("%c"), _end_date.strftime("%c"))

# print(datetime.datetime.today().isoweekday())