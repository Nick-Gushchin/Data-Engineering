import numpy as np
from datetime import datetime, timedelta


num_transactions = 5
transaction_ids = np.arange(1, num_transactions + 1)
user_ids = np.array([101, 102, 103, 101, 104])
product_ids = np.array([201, 202, 203, 204, 205])
quantities = np.array([1, 2, 0, 3, 2])
prices = np.array([19.5, 29.0, 9.0, 49.5, 15.0])

start_date = datetime(2024, 8, 1)
timestamps = [start_date + timedelta(days=i) for i in range(num_transactions)]

timestamps = [ts.strftime('%Y-%m-%d %H:%M:%S') for ts in timestamps]

dtype = [('transaction_id', 'i4'),
         ('user_id', 'i4'),
         ('product_id', 'i4'),
         ('quantity', 'i4'),
         ('price', 'f4'),
         ('timestamp', 'U19')]

data = np.array(list(zip(transaction_ids, user_ids, product_ids, quantities, prices, timestamps)), dtype=dtype)


def calculate_total_revenue(array):
    total_revenue = np.sum(array['quantity'] * array['price'])
    
    return total_revenue


def count_unique_users(array):
    
    unique_users = np.unique(array['user_id'])
    num_unique_users = len(unique_users)
    
    return num_unique_users


def most_purchased_product(array):
    product_ids = array['product_id']
    quantities = array['quantity']
    product_quantity_map = {}
    for product_id, quantity in zip(product_ids, quantities):
        if product_id in product_quantity_map:
            product_quantity_map[product_id] += quantity
        else:
            product_quantity_map[product_id] = quantity
    
    most_purchased_product = max(product_quantity_map, key=product_quantity_map.get)
    
    return most_purchased_product


def convert_prices_to_int(array):
    prices_float = array['price']
    prices_int = np.round(prices_float).astype(int)
    new_array = np.copy(array)
    new_array['price'] = prices_int
    
    return new_array


def check_column_dt(array):
    dtype_info = array.dtype
    column_data_types = {name: dtype_info[name].str for name in dtype_info.names}
    
    return column_data_types


def product_and_quantity(array):
    product_ids = array['product_id']
    quantities = array['quantity']
    new_array = np.array(list(zip(product_ids, quantities)))
    
    return new_array


def transaction_per_user(array):

    user_ids = array['user_id']
    unique_user_ids, counts = np.unique(user_ids, return_counts=True)
    transaction_counts = np.array(list(zip(unique_user_ids, counts)))
    
    return transaction_counts


def mask_zero_quantity(array):
    
    quantity = array['quantity']
    mask = quantity == 0
    masked_array = np.ma.masked_where(mask, array)
    
    return masked_array


def increase_prices(array, percentage):
    increase = 1 + (percentage / 100.0)
    prices = array['price']
    new_prices = prices * increase
    new_array = np.copy(array)
    new_array['price'] = new_prices
    
    return new_array


def filter_transaction(array):
    quantities = array['quantity']
    mask = quantities > 1
    new_array = array[mask]
    
    return new_array


def extract_transactions_by_user(array, user_id):
    mask = array['user_id'] == user_id
    user_transactions = array[mask]
    
    return user_transactions


def filter_transactions_by_date(array, start_date, end_date):

    start_date = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end_date, '%Y-%m-%d %H:%M:%S')
    timestamps = np.array([datetime.strptime(ts, '%Y-%m-%d %H:%M:%S') for ts in array['timestamp']])
    mask = (timestamps >= start_date) & (timestamps <= end_date)
    filtered_array = array[mask]
    
    return filtered_array


def top_products(array, top_n=5):
    product_ids = np.unique(array['product_id'])
    total_revenues = {}
    for product_id in product_ids:
        mask = array['product_id'] == product_id
        revenue = np.sum(array['price'][mask] * array['quantity'][mask])
        total_revenues[product_id] = revenue

    top_products = sorted(total_revenues.items(), key=lambda x: x[1], reverse=True)[:top_n]
    top_product_ids = [product_id for product_id, _ in top_products]
    top_products_mask = np.isin(array['product_id'], top_product_ids)
    top_products_transactions = array[top_products_mask]
    
    return top_products_transactions


def print_array(array, message="Array:"):
    print(message)
    print(array)


print('Total Revenue Function:', calculate_total_revenue(data))
print('Unique Users Function:', count_unique_users(data))
print('Most Purchased Product Function:', most_purchased_product(data))
print_array(convert_prices_to_int(data), 'Type Casting:')
print_array(check_column_dt(data), 'Checking Function:')
print_array(product_and_quantity(data), 'Product Quantity Array Function:')
print_array(transaction_per_user(data), 'User Transaction Count Function:')
print_array(mask_zero_quantity(data), 'Masked Array Function:')
print_array(increase_prices(data, 5), 'Price Increase Function:')
print_array(filter_transaction(data), 'Filter Transactions Function:')
print_array(extract_transactions_by_user(data, 101), 'User Transactions Function:')
print_array(filter_transactions_by_date(data, '2024-08-01 00:00:00' , '2024-08-03 00:00:00'), 'Date Range Slicing Function:')
print_array(top_products(data, 3), 'Top Products Function:')