from worker import worker

register = worker.Pickler().register

listenr = worker.Pickler().listener()
def func1(a, b, c, d=1):
    print(a, b, c, d)

def func2(a=1, b=1, c=1, d=1):
    print(a, b, c, d)

register(func1, args=(1, 2, 3), kwargs={"d": 10})


# import datetime
#
# anytime_str = '2023-03-08 23:06:00'  # Example anytime string
# anytime_obj = datetime.datetime.strptime(anytime_str, '%Y-%m-%d %H:%M:%S')  # Convert to datetime object
# timestamp = datetime.datetime.timestamp(anytime_obj)  # Convert to Unix timestamp
#
# register(func1, kwargs={"a" : 1, "b" : 2, "c" : 12}, time=timestamp)