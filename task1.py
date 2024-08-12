import numpy as np

a = np.arange(1,11,)
b = np.arange(1, 10).reshape(3, 3)

def print_array(array, message="Array:"):
    print(message)
    print(array)

third_element = a[2]
print_array(third_element)
sliced = b[:2, :2]
print_array(sliced)

add5 = a + 5
print_array(add5)
multiply = b * 2
print_array(multiply)