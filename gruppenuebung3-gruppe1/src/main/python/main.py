
import numpy as np
import csv
import math
import pickle
import os
from sklearn import manifold
from sklearn.decomposition import PCA

first = True
class Embedding:

    def __init__(self, vecs, vocab):
        self.m = vecs
        self.iw = vocab
    @classmethod
    def load(cls, path):
        mat = np.load(path + "-w_umap5_normalized.npy", mmap_mode="c")
        iw = pickle.load(open(path + "-vocab.pkl", "rb"),encoding='latin1')
        return cls(mat, iw) 

    def writeCSV(self,year):
        global first
        filename='out'
        filename = filename + '-normalized'	
        filename = filename + '.csv'		
        if os.path.exists(filename) and first:
            os.remove(filename)		
        with open(filename, "a+",encoding='utf-8',newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=';')
            dims = ['word']
            #creating the headers
            for x in range(0, 5):
                dims.append("dim" +str(x+1))
            if  first:
                writer.writerow((dims))
            first=False
            #creating the lines with content
            #for i in range(0, len(self.m)):			
            for i in range(0, len(self.m)):
                line = []
                content = self.iw[i]
                line.append(content)
                flatten  = self.m[i].flatten()			
                line.extend(flatten)
                line.append(year)
                writer.writerow((line))

if __name__ == '__main__':
    for i in range(1900,1990+1,10):
        Embedding.load("source/" + str(i)).writeCSV(i)
