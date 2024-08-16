import pandas as pd

df = pd.read_csv('data.csv')


def print_dataframe_info(df, message='DataFrame Information:'):
    print(message)
    print('\nBasic DataFrame Info:')
    print(df.info())
    
    print('\nMissing Values Per Column:')
    print(df.isnull().sum())
    
    print('\nNumber of Entries:')
    print(len(df))'
    
    print('\nSample Data (First 5 Rows):')
    print(df.head())

print_dataframe_info(df, 'Before cleaning')


df['name'].fillna('Unknown', inplace=True)
df['host_name'].fillna('Unknown', inplace=True)
df['last_review'].fillna('NaT', inplace=True)


price_conditions = [
    (df['price'] < 100),
    (df['price'] >= 100) & (df['price'] < 300),
    (df['price'] >= 300)
]
price_categories = ['Low', 'Medium', 'High']
df['price_category'] = pd.cut(df['price'], bins=[-float('inf'), 100, 300, float('inf')], labels=price_categories)


stay_conditions = [
    (df['minimum_nights'] <= 3),
    (df['minimum_nights'] >= 4) & (df['minimum_nights'] <= 14),
    (df['minimum_nights'] > 14)
]
stay_categories = ['Short-term', 'Medium-term', 'Long-term']
df['length_of_stay_category'] = pd.cut(df['minimum_nights'], bins=[-float('inf'), 3, 14, float('inf')], labels=stay_categories)


missing_critical_columns = df[['name', 'host_name', 'last_review']].isnull().sum()
print('Missing values in critical columns:', missing_critical_columns)


zero_price_rows = df[df['price'] == 0]
print('Rows with price equal to 0:', zero_price_rows)
df = df[df['price'] > 0]
price_check = df['price'].min() > 0
print('\nAll price values greater than 0:', price_check)

print_dataframe_info(df, 'After cleaning')