import json
import time


def load_config(config_location):
    with open(config_location, 'r') as file:
        return json.load(file)
    

def time_function(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} executed in {end_time - start_time} seconds")
        return result
    return wrapper
