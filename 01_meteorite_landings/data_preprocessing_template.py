import numpy as py
import matplotlib.pyplot as plt
import pandas as pd
#from sklearn.impute import SimpleImputer
#from sklearn.preprocessing import LabelEncoder, Normalizer, OneHotEncoder, StandardScaler
#from sklearn.compose import ColumnTransformer
#from sklearn.model_selection import train_test_split


dataset = pd.read_json("meteorite_landings_50000.dat")
dataset.drop(["id"], inplace=True, axis=1)
sample = dataset.head()

year1 = pd.to_datetime(dataset["year"], errors = 'coerce')
year2 = year1[year1.notnull()].dt.year

y = year2.groupby(year2).value_counts()
y.hist(bins=10)

y3 = year2[year2 > 3000]

print(dataset.columns)



# """
# Import missing data
# """
# dataset = pd.read_csv("Data.csv")
# X = dataset.iloc[:, :-1].values
# 
# # How do we show variable without DataFrame
# xdf = pd.DataFrame(data=X, columns=["Country","Age","Salary"])
# 
# Y = dataset.iloc[:, 3].values
# ydf = pd.DataFrame(data=Y, columns=["Purchased"])
# 
# """
# Impute missing data
# """
# # mean = sum of all values divided by number of elements
# imputer = SimpleImputer(missing_values=py.nan, strategy="mean")
# imputer = imputer.fit(X[:, 1:3])
# 
# X[:, 1:3] = imputer.transform(X[:, 1:3])
# 
# """
# Categorize data i.e. convert categories to numerical values
# """
# ct_X = ColumnTransformer(
#     transformers=[
#         ("onehot", OneHotEncoder(sparse=False), [0])
#     ], 
#     remainder="passthrough" )
# X = ct_X.fit_transform(X)
# 
# labelencoder = LabelEncoder()
# Y = labelencoder.fit_transform(Y)
# 
# """
# Splitting the dataset into the training and test set
# """
# X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=0)
# 
# sc_X = StandardScaler()
# X_train = sc_X.fit_transform(X_train)
# X_test = sc_X.transform(X_test)
