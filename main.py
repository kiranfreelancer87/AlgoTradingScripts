import pandas as pd

df = pd.read_csv('ind_nifty500list.csv')

print(df['Symbol'])
