import time


def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed_time = end_time - start_time
        function_name = func.__name__
        print(f"{function_name} took {elapsed_time:.4f} seconds")
        return result
    return wrapper


def concatenate_subarrays(arrays):
    result = []
    for subarray in arrays:
        result.extend(subarray)
    return result
