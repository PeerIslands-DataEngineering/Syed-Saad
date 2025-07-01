import numpy as np
s = np.random.uniform(100, 500, (30, 5))
print(s)

a = np.mean(s)
print("average",a)

max = np.max(s)
print("max",max)

min =np.min(s)
print("min",min)

nor = (s-min//max-min)
print("normalize",nor)

r = np.any(s < 200)
print(r)

