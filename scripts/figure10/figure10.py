import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import warnings; warnings.filterwarnings(action='once')

large = 22; med = 16; small = 12
params = {'axes.titlesize': large,
          'legend.fontsize': med,
          'figure.figsize': (16, 10),
          'axes.labelsize': med,
          'axes.titlesize': med,
          'xtick.labelsize': med,
          'ytick.labelsize': med,
          'figure.titlesize': large}
plt.rcParams.update(params)
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_style("white")

df = pd.read_csv('cloudts-production-latency.csv')
mycolors = ['tab:red', 'tab:blue', 'tab:green', 'tab:orange']    
 
columns = df.columns[1:]  

plt.figure(figsize=(8,5), dpi= 80)

x = np.array([0, 6])
y = np.array([0, 100])

plt.subplot(2, 1, 1)


i=0
column=columns[i]
l0,=plt.plot(df.time.values, df[column].values, lw=1.5, color=mycolors[0])    

i=1
column=columns[i]
l2,=plt.plot(df.time.values, df[column].values, lw=1.5, color=mycolors[2])    

 
plt.legend([l0,l2],['CloudTS','Baseline'], frameon=True, loc='upper right')
plt.ylabel('Query Latency (s)')
plt.title("cpu_avg")
plt.xticks(range(0, 120), [])

plt.subplot(2, 1, 2)

i=2
column=columns[i]
l4,=plt.plot(df.time.values, df[column].values, lw=1.5, color=mycolors[0])    


i=3
column=columns[i]
l6,=plt.plot(df.time.values, df[column].values, lw=1.5, color=mycolors[2])    


plt.legend([l4,l6,],['CloudTS','Baseline'], frameon=True, loc='upper right')
plt.title("memory_usage_avg")
plt.ylabel('Query Latency (s)')
plt.xlabel('Time(min)')

plt.subplots_adjust(wspace=0, hspace=0.2)
plt.savefig("./figure10.pdf", bbox_inches='tight')
plt.show()