from datetime import datetime

def right_now():
    """Return a formatted string version of datetime.now()"""
    return datetime.now().strftime('%m-%d-%Y:%H:%M:%S.%f')
