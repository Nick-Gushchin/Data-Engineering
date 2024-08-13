import numpy as np


def print_array(array, message="Array:"):
    print(message)
    print(array)

array = np.random.randint(0, 100, size=(10, 10))

def save_array(array, filename):
    text_filename = f"{filename}.txt"
    np.savetxt(text_filename, array, fmt='%d')

    csv_filename = f"{filename}.csv"
    np.savetxt(csv_filename, array, delimiter=',', fmt='%d')

    npy_filename = f"{filename}.npy"
    np.save(npy_filename, array)

save_array(array, 'ArrayFile')

def load_txt(filename):
    text_filename = f"{filename}.txt"
    return np.loadtxt(text_filename, dtype=int)

def load_csv(filename):
    csv_filename = f"{filename}.csv"
    return np.loadtxt(csv_filename, delimiter=',', dtype=int)

def load_npy(filename):
    npy_filename = f"{filename}.npy"
    return np.load(npy_filename)

print_array(array)
print_array(load_txt('ArrayFile'), 'Loaded from txt')
print_array(load_csv('ArrayFile'), 'Loaded from csv')
print_array(load_npy('ArrayFile'), 'Loaded from npy')

sum = np.sum(array)
print_array(sum, 'Summation:')

mean = np.mean(array)
print_array(mean, 'Mean:')

median = np.median(array)
print_array(median, 'Median:')

std = np.std(array)
print_array(std, 'STD:')