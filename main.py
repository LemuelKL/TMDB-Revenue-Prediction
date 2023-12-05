# TMDB Box Office Prediction
# Description: In this competition, you're presented with metadata on over 7,000 past films from The Movie Database to try and predict their overall worldwide box office revenue. Data points provided include cast, crew, plot keywords, budget, posters, release dates, languages, production companies, and countries.

# Importing the libraries
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

# Importing the dataset
train = pd.read_csv('train.csv')
test = pd.read_csv('test.csv')

# Data Exploration
train.head()
train.info()
train.describe()

# Data Visualization
plt.figure(figsize=(16,8))
plt.subplot(1,2,1)
plt.scatter(range(train.shape[0]), np.sort(train.revenue.values))
plt.xlabel('index', fontsize=12)
plt.ylabel('revenue', fontsize=12)
plt.subplot(1,2,2)
sns.distplot(train.revenue.values, bins=50, kde=True)
plt.xlabel('revenue', fontsize=12)
plt.show()
