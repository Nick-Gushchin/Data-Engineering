import numpy as np


def print_array(array, message="Array:"):
    print(message)
    print(array)

array = np.random.randint(1, 101, size=(6, 6))
print_array(array)

transposed = np.transpose(array)
print_array(transposed, 'Transposed array:')

reshaped = np.reshape(array, (3, 12))
print_array(reshaped, 'Reshaped array:')

arrays = np.array_split(array, 3, 0)
print_array(arrays, 'Splited array:')

concatenated = np.concatenate(arrays, 0)
print_array(concatenated, 'Concatenated array:')