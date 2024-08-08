import pandas as pd

arr_data = [ [1, 10, 100], [2, 20, 200]]
print("Dataframe created from array")
print(arr_data)
print("---------------------")

df = pd.DataFrame(arr_data)
print("The DataFrame ")
print(df)