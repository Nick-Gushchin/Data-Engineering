import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df = pd.read_csv('data.csv')

grouped_df = df.groupby(['neighbourhood_group', 'room_type']).agg(
    mean_availability=('availability_365', 'mean'),
    std_availability=('availability_365', 'std')
).reset_index()

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
    
    plt.show()


def plot_grouped_bar_availability(df):
    neighbourhood_groups = df['neighbourhood_group'].unique()
    room_types = df['room_type'].unique()

    x = np.arange(len(neighbourhood_groups))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 8))

    for i, room_type in enumerate(room_types):
        subset = df[df['room_type'] == room_type]
        ax.bar(
            x + i * width,
            subset['mean_availability'],
            width,
            label=room_type,
            yerr=subset['std_availability'],
            capsize=5
        )

    ax.set_title('Average Availability by Room Type Across Neighborhoods')
    ax.set_xlabel('Neighborhood Group')
    ax.set_ylabel('Average Availability (Days)')
    ax.set_xticks(x + width / 2)
    ax.set_xticklabels(neighbourhood_groups)
    ax.legend(title='Room Type')

    plt.show()



plot_bar_distribution(df)
plot_box_price_distribution(df)
plot_grouped_bar_availability(grouped_df)