import pandas as pd

# Load the CSV file into a pandas DataFrame
df = pd.read_csv('LOCAL_PEOPLE_DONG_202304.csv')

# Reset the index
df.reset_index(inplace=True)

# Define column names based on the number of columns in df
column_names = ['기준일ID', '시간대구분', '행정동코드', '총생활인구수', '남자0세부터9세생활인구수',
              '남자10세부터14세생활인구수', '남자15세부터19세생활인구수', '남자20세부터24세생활인구수',
              '남자25세부터29세생활인구수', '남자30세부터34세생활인구수', '남자35세부터39세생활인구수',
              '남자40세부터44세생활인구수', '남자45세부터49세생활인구수', '남자50세부터54세생활인구수',
              '남자55세부터59세생활인구수', '남자60세부터64세생활인구수', '남자65세부터69세생활인구수',
              '남자70세이상생활인구수', '여자0세부터9세생활인구수', '여자10세부터14세생활인구수',
              '여자15세부터19세생활인구수', '여자20세부터24세생활인구수', '여자25세부터29세생활인구수',
              '여자30세부터34세생활인구수', '여자35세부터39세생활인구수', '여자40세부터44세생활인구수',
              '여자45세부터49세생활인구수', '여자50세부터54세생활인구수', '여자55세부터59세생활인구수',
              '여자60세부터64세생활인구수', '여자65세부터69세생활인구수', '여자70세이상생활인구수']

if len(df.columns) == len(column_names) + 1:
    column_names.insert(0, 'index')

# Assign new column names
df.columns = column_names

# Replace all 'NaN' values with 0
df.fillna(0, inplace=True)

# Convert the necessary columns into integers
for column in df.columns: 
    try:
        df[column] = df[column].astype(int)
    except ValueError:
        print(f"Could not convert column {column} to integer.")

# Save the DataFrame back to CSV
df.to_csv('LOCAL_PEOPLE_DONG_202304_reduced.csv', index=False)
