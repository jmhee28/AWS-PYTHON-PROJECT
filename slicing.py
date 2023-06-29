import pandas as pd

# Load the data
df = pd.read_csv('LOCAL_PEOPLE_DONG_202304_reduced.csv')

# Get unique dates
dates = df['기준일ID'].unique()

# For each unique date, create a new DataFrame and save to a csv file
for date in dates:
    df_date = df[df['기준일ID'] == date]
    df_date.to_csv(f'{date}.csv', index=False)
