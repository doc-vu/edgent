import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit

def func(X):
  p,s=X
  print(p)
  print(s)

if __name__=="__main__":
  P=[np.genfromtxt('output.csv',dtype='int',delimiter=',',usecols=[0],skip_header=1)]
  S=[np.genfromtxt('output.csv',dtype='int',delimiter=',',usecols=[1],skip_header=1)]
  Y=[np.genfromtxt('output.csv',dtype='float',delimiter=',',usecols=[2],skip_header=1)]
  func((P,S))
