import pandas as pd

df = pd.read_csv('data.csv')

pivot_summary = pd.pivot_table(
    df,
    values='price',
    index='neighbourhood_group',
    columns='room_type',
    aggfunc='mean'
)


def classify_availability(days):
    if days < 50:
        return 'Rarely Available'
    elif 50 <= days <= 200:
        return 'Occasionally Available'
    else:
        return 'Highly Available'

df['availability_status'] = df['availability_365'].apply(classify_availability)

availability_stats = df.groupby('availability_status').agg({
    'price': ['mean', 'median', 'std'],
    'number_of_reviews': ['mean', 'median', 'std'],
    'neighbourhood_group': 'count'
})


correlation_matrix = df[['availability_365', 'price', 'number_of_reviews']].corr()


descriptive_stats = df[['price', 'minimum_nights', 'number_of_reviews']].agg({
    'price': ['mean', 'median', 'std'],
    'minimum_nights': ['mean', 'median', 'std'],
    'number_of_reviews': ['mean', 'median', 'std']
})


df['last_review'] = pd.to_datetime(df['last_review'])
df.set_index('last_review', inplace=True)


monthly_trends = df.resample('M').agg({
    'number_of_reviews': 'sum',
    'price': 'mean'
})


def print_analysis_results(df, message='Analysis Results'):
    print(message)
    print(df)


print_analysis_results(availability_stats, 'Classified Listings by Availability:')
print_analysis_results(correlation_matrix, 'Correlation Matrix:')
print_analysis_results(descriptive_stats, 'Descriptive Statistics:')
print_analysis_results(monthly_trends, 'Time Series Analysis:')
monthly_trends.to_csv('time_series_airbnb_data.csv')