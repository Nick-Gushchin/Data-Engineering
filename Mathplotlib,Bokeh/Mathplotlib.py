import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

df = pd.read_csv('data.csv')
df['last_review'] = pd.to_datetime(df['last_review'])
def apply_rolling_average(df, window=2):
    df = df.sort_values(by='last_review')
    df['rolling_avg_reviews'] = df['number_of_reviews'].rolling(window=window).mean()
    return df

df = df.groupby('neighbourhood_group').apply(apply_rolling_average).reset_index(drop=True)

grouped_df = df.groupby(['neighbourhood_group', 'room_type'])['availability_365'].agg(['mean', 'std']).unstack()
h_data = df.pivot_table(index='neighbourhood_group', values=['price', 'availability_365'], aggfunc='mean')
stacked_data = df.pivot_table(index='neighbourhood_group', columns='room_type', values='number_of_reviews', aggfunc='sum').fillna(0)

def plot_bar_distribution(df):

    neighbourhood_counts = df['neighbourhood_group'].value_counts()
    
    plt.figure(figsize=(10, 6))
    bars = plt.bar(neighbourhood_counts.index, neighbourhood_counts.values, color=['skyblue', 'orange', 'green', 'red', 'purple'])
    
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval + 0.2, int(yval), ha='center', va='bottom')
    
    plt.title('Distribution of Listings Across Neighbourhood Groups')
    plt.xlabel('Neighbourhood Group')
    plt.ylabel('Number of Listings')
    
    plt.savefig('1.png')
    plt.show()


def plot_box_price_distribution(df):
    
    plt.figure(figsize=(12, 8))
    

    ax = df.boxplot(column='price', by='neighbourhood_group', patch_artist=True, grid=False)


    colors = ['skyblue', 'orange', 'green', 'red', 'purple']
    for patch, color in zip(ax.artists, colors):
        patch.set_facecolor(color)
    
    plt.title('Distribution of Prices by Neighbourhood Group')
    plt.suptitle('')
    plt.xlabel('Neighbourhood Group')
    plt.ylabel('Price')
    

    for flier in ax.get_lines():
        flier.set(marker='o', color='red', alpha=0.5)
    
    plt.savefig('2.png')
    plt.show()


def plot_grouped_bar(df):
    means = df['mean']
    errors = df['std']
    
    fig, ax = plt.subplots(figsize=(12, 8))
    
    bar_width = 0.35
    index = np.arange(len(means))
    
    bars1 = ax.bar(index - bar_width/2, means['Entire home/apt'], bar_width, yerr=errors['Entire home/apt'], label='Entire home/apt', color='skyblue', capsize=5)
    bars2 = ax.bar(index + bar_width/2, means['Private room'], bar_width, yerr=errors['Private room'], label='Private room', color='orange', capsize=5)
    
    ax.set_title('Average Availability by Room Type Across Neighborhood Groups')
    ax.set_xlabel('Neighbourhood Group')
    ax.set_ylabel('Average Availability (365 days)')
    ax.set_xticks(index)
    ax.set_xticklabels(means.index)
    ax.legend()

    plt.savefig('3.png')
    plt.show()


def plot_scatter_with_regression(df):
    plt.figure(figsize=(10, 6))

    markers = {'Entire home/apt': 'o', 'Private room': 's', 'Shared room': 'D'}
    colors = {'Entire home/apt': 'skyblue', 'Private room': 'orange', 'Shared room': 'green'}

    for room_type in df['room_type'].unique():
        subset = df[df['room_type'] == room_type]
        plt.scatter(subset['price'], subset['number_of_reviews'], 
                    marker=markers[room_type], color=colors[room_type], label=room_type)

    sns.regplot(x='price', y='number_of_reviews', data=df, scatter=False, color='black', line_kws={"linewidth":1})

    plt.title('Scatter Plot of Price vs. Number of Reviews')
    plt.xlabel('Price ($)')
    plt.ylabel('Number of Reviews')
    plt.legend(title='Room Type')

    plt.savefig('4.png')
    plt.show()


def plot_reviews_trend(df):
    plt.figure(figsize=(12, 8))
    
    colors = {'Manhattan': 'blue', 'Brooklyn': 'orange', 'Queens': 'green', 'Bronx': 'red', 'Staten Island': 'purple'}

    for neighbourhood_group in df['neighbourhood_group'].unique():
        subset = df[df['neighbourhood_group'] == neighbourhood_group]
        plt.plot(subset['last_review'], subset['rolling_avg_reviews'], label=neighbourhood_group, color=colors[neighbourhood_group])

    plt.title('Trend of Number of Reviews Over Time by Neighbourhood Group')
    plt.xlabel('Last Review Date')
    plt.ylabel('Rolling Average of Number of Reviews')
    plt.legend(title='Neighbourhood Group')
    
    plt.savefig('5.png')
    plt.show()

def plot_heatmap(heatmap_data):
    plt.figure(figsize=(10, 8))

    sns.heatmap(heatmap_data, annot=True, fmt=".1f", cmap='YlGnBu', cbar=True)

    plt.title('Heatmap of Price and Availability by Neighbourhood Group')
    plt.xlabel('Metric')
    plt.ylabel('Neighbourhood Group')

    plt.savefig('6.png')
    plt.show()

def plot_stacked_bar(stacked_data):
    plt.figure(figsize=(12, 8))

    stacked_data.plot(kind='bar', stacked=True, color=['skyblue', 'orange', 'green'], ax=plt.gca())

    plt.title('Number of Reviews by Room Type Across Neighbourhood Groups')
    plt.xlabel('Neighbourhood Group')
    plt.ylabel('Number of Reviews')
    plt.legend(title='Room Type', bbox_to_anchor=(1.05, 1), loc='upper left')

    plt.savefig('7.png')
    plt.show()


plot_bar_distribution(df)
plot_box_price_distribution(df)
plot_grouped_bar(grouped_df)
plot_scatter_with_regression(df)
plot_reviews_trend(df)
plot_heatmap(h_data)
plot_stacked_bar(stacked_data)