#!/usr/bin/env python
# coding: utf-8

# In[1]:


from random import random
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Spark Pi")#.setMaster("local[1]")
sc = SparkContext.getOrCreate(conf=conf)


# In[4]:


NUM_SAMPLES = 10000

# generate random numbers on the [-1, 1] interval
pick_random = lambda: random() * 2 - 1

def inside(n):
    x, y = pick_random(), pick_random()
    return (x * x + y * y) < 1

count = sc.            parallelize(range(0, NUM_SAMPLES)).            filter(inside).            count()

approx_pi = count * 4.0 / NUM_SAMPLES

print(f"Pi is approximately {approx_pi}")


# ## Challenge
# Implement the Leibniz formula for estimation of $\pi$:
# 
# $$
# \frac{\pi}{4} = \sum_{k=0}^{\infty}\frac{(-1)^k}{2k + 1}
# $$

# In[5]:


leib_pi = 4 * sc.parallelize(range(10000)).            map(lambda x: ((-1) ** x)/(2 * x + 1) ).            reduce(lambda x, y: x + y)

print(f"Approximation of pi: {leib_pi}")


# In[ ]:




