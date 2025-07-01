import pandas as pd
import numpy as np

data = {
    'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', np.nan],
    'Age': [25, 30, 35, 40, np.nan, 22],
    'Salary': [50000, 60000, 70000, 80000, 90000, 40000],
    'Department': ['HR', 'IT', 'IT', 'Finance', 'HR', 'Finance']
}
df = pd.DataFrame(data)

print(df.head())
print(df.tail(2))
print(df.info())
print(df.describe())
print(df.columns)
print(df.shape)
print(df.dtypes)

print(df['Name'])
print(df[['Name', 'Salary']])
print(df.iloc[0])
print(df.loc[0, 'Name'])

print(df.isnull())
print(df.dropna())
print(df.fillna('Unknown'))

print(df[df['Age'] > 30])
print(df.sort_values(by='Salary', ascending=False))
print(df.groupby('Department')['Salary'].mean())
print(df.agg({'Age': 'mean', 'Salary': ['min', 'max']}))
print(df.rename(columns={'Salary': 'Income'}))
print(df.duplicated())
print(df.drop_duplicates())
print(df.value_counts('Department'))
print(df.set_index('Name'))
print(df.reset_index())
print(df.sort_index())
print(df.sample(3))
print(df.memory_usage())
print(df.nunique())
print(df.apply(np.mean, numeric_only=True))
print(df.select_dtypes(include='number'))
print(df.corr(numeric_only=True))
print(df.to_dict())
print(df.to_numpy())
print(df.copy())
print(pd.concat([df, df]))
print(pd.merge(df, df, on='Department', suffixes=('_1', '_2')))
print(df.pivot_table(index='Department', values='Salary', aggfunc='mean'))
