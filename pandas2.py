import pandas as pd

data = {
    'Student': ['John', 'Sara', 'Mike', 'Anna', 'David', 'Emily', 'Chris', 'Sophia'],
    'Subject': ['Math', 'Science', 'Math', 'Science', 'Math', 'Science', 'Math', 'Science'],
    'Marks': [85, 72, 90, 65, 78, 88, 92, 55],
    'Attendance': [92, 80, 95, 70, 85, 90, 97, 60]
}

df = pd.DataFrame(data)

print(df)

a = df.groupby('Subject')['Marks'].mean()
print("AVG marks",a)

h = df[(df['Marks'] > 85) & (df['Attendance'] < 90)]
print("Highest",h)