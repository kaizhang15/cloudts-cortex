import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import warnings; warnings.filterwarnings(action='once')

df = pd.read_csv('cloudts-first-arrive.csv')

mycolors = ['tab:red', 'tab:orange', 'tab:blue', 'tab:green', 'tab:brown']    


fig, ax = plt.subplots(1,1,figsize=(8, 4), dpi= 80)    

columns = df.columns[1:]  
for i, column in enumerate(columns):
    if (i==0):
        l1=plt.plot(df.date.values, df[column].values, lw=1.5, color=mycolors[i], label='Baseline')
    if (i==1):
        l4=plt.plot(df.date.values, df[column].values, lw=1.5, color=mycolors[i], label='CloudTS')   
ax.legend(loc="upper right", frameon=True)


plt.hlines(0.1, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(0.2, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(0.3, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(0.4, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(0.5, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(0.6, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(0.7, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(0.8, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(0.9, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(1.0, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(2.0, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(3.0, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(4.0, xmin=-6, xmax=-1, colors='black', lw=0.8)
plt.hlines(5.0, xmin=-6, xmax=-1, colors='black', lw=0.8)
 
plt.ylim(0.07, 0.2)
plt.xlim(-6, 245)    




from matplotlib.ticker import ScalarFormatter
for axis in [ax.yaxis]:
    axis.set_major_formatter(ScalarFormatter())
ax.set_yticks([0.1,0.15, 0.2,])
ax.set_xticks([0, 30, 60, 90, 120, 150, 180, 210, 240])
plt.ylabel('Latency (s)')
plt.xlabel('Time (min)')
plt.savefig("./figure12.pdf", bbox_inches='tight')
plt.show()