import pandas as pd

df = pd.read_csv('data.csv')


price_conditions = [
    (df['price'] < 100),
    (df['price'] >= 100) & (df['price'] < 300),
    (df['price'] >= 300)
]
price_categories = ['Low', 'Medium', 'High']
df['price_category'] = pd.cut(df['price'], bins=[-float('inf'), 100, 300, float('inf')], labels=price_categories)


filtered_df = df.loc[df['neighbourhood_group'].isin(['Manhattan', 'Brooklyn'])]
filtered_df = filtered_df.loc[(filtered_df['price'] > 100) & (filtered_df['number_of_reviews'] > 10)]
filtered_df = filtered_df.loc[:, ['neighbourhood_group', 'price', 'minimum_nights', 'number_of_reviews', 'price_category', 'availability_365']]


grouped_df = filtered_df.groupby(['neighbourhood_group', 'price_category'])
grouped_df = grouped_df.agg({
    'price': 'mean',
    'minimum_nights': 'mean',
    'number_of_reviews': 'mean',
    'availability_365': 'mean'
})


sorted_df = filtered_df.sort_values(by=['price', 'number_of_reviews'], ascending=[False, True])


neighborhood_ranking = filtered_df.groupby('neighbourhood_group').agg(
    total_listings=('price', 'size'),
    average_price=('price', 'mean')
)
neighborhood_ranking = neighborhood_ranking.sort_values(by=['total_listings', 'average_price'], ascending=[False, True])


def print_grouped_data(df, message='Grouped Data:'):
    print(message)
    print(df)

print_grouped_data(grouped_df, 'Average data:')
print_grouped_data(sorted_df, 'Sorted data:')
print_grouped_data(neighborhood_ranking, 'Neighbourhood ranking:')
sorted_df.to_csv('aggregated_airbnb_data.csv')