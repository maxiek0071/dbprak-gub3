
import numpy as np
import csv
import math
import pickle
from sklearn import manifold
from sklearn.decomposition import PCA


class Embedding:

    def __init__(self, vecs, vocab):
        self.m = vecs
        self.iw = vocab
    @classmethod
    def load(cls, path):
        mat = np.load(path + "-w_umap5_normalized.npy", mmap_mode="c")
        iw = pickle.load(open(path + "-vocab.pkl", "rb"),encoding='latin1')
        return cls(mat, iw) 

    def writeCSV(self,normalize = False, mds = False):
        if mds:
            self.m=np.resize(self.m,(20000,300))
            mds = manifold.MDS(n_components=5,n_jobs=1,n_init=1)
            self.m = mds.fit(self.m).embedding_
        filename='out'
        if normalize:
            filename = filename + '-normalized'
        else:
            filename = filename + '-unnormalized'	
        filename = filename + '.csv'			
        with open(filename, "w",encoding='utf-8',newline='') as csv_file:
            writer = csv.writer(csv_file, delimiter=';')
            dims = ['word']
            #creating the headers
            for x in range(0, 5):
                dims.append("dim" +str(x+1))
            dims.append("vector_length")
            writer.writerow((dims))
            #creating the lines with content
            #for i in range(0, len(self.m)):			
            for i in range(0, len(self.m)):
                line = []
                content = self.iw[i]
                line.append(content)
                flatten  = self.m[i].flatten()
                sqrLength = 0
                for j in range(0,len(flatten)):
                    sqrLength = sqrLength + flatten[j] ** 2
                sqrLength=math.sqrt(sqrLength)				
                line.extend(flatten)
                line.append(sqrLength)
                writer.writerow((line))

if __name__ == '__main__':
    embedding = Embedding.load("source/" + str(1990))
    embedding.writeCSV(mds=False)