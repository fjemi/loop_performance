# data analysis/wrangling
#import modin.pandas as pd
import pandas as pd
import numpy as np
# data modeling
from dataclasses import dataclass, asdict, field
from typing import Dict, List
# loop progress
from tqdm import trange, tqdm
# date
from datetime import datetime
# Web/JSON
from flask import jsonify
import json


def add(a, b):
    '''Calculate the sum of two integers'''
    return (a + b) * 2

def pythagorean(a, b):
    '''Calculate the hypotnuse of a triangle given the lengths of side a, b'''
    return sqrt(a**2 + b**2)

# add timeit decorater


@dataclass
class Result:
    size: int
    loop_type: str
    execution_time: float
    
 
@dataclass
class Loop:
    size: int = None
    df: pd.DataFrame = field(default_factory=None, repr=False), 
    results: List = field(default_factory=list)
    result: List[Result] = field(default_factory=list, repr=False)
    
    
    def set_df(self, size):
        '''Create or load pickled dataframe after initilization'''
        try:            
            # Load pickled df if exists
            df = pd.read_pickle('df{}.pkl'.format(size))
        
        except Exception as e:
            # Create dataframe with random numbers
            df = pd.DataFrame(np.random.randint(0, 1000, size=(size, 2)), columns=list('ab'))
            # Pickle pandas object
            df.to_pickle('df.pkl')
            
        finally:
            df['c'] = 0
            self.df = df
            self.size = size
            
        
    def use_list_comprehension(self):
        '''Use for loop to iterate'''
        start = datetime.utcnow()
        
        self.df.c = [add(a, b) for a, b in zip(self.df.a, self.df.b)]
        
        runtime = (datetime.utcnow() - start).total_seconds()
        result = Result(self.size, 'list comprehension', runtime)
        self.result.append(result)
            
    
    def use_for(self):
        '''Use for loop to iterate'''
        start = datetime.utcnow()
        
        for i in range(0, len(self.df)):
            self.df.c.iloc[i] = add(self.df.a.iloc[i], self.df.a.iloc[i])
            
        runtime = (datetime.utcnow() - start).total_seconds()
        
        result = {'loop': 'for', 'runtime': runtime}
        self.results.append(result)
        
        
    def use_while(self):
        '''Use while loop to iterate'''
        start = datetime.utcnow()
        
        i = len(self.df) - 1
        
        while i != 0:
            self.df.c.iloc[i] = add(self.df.a.iloc[i], self.df.a.iloc[i])
            i = i - 1
            
        runtime = (datetime.utcnow() - start).total_seconds()
        
        result = {'loop': 'while', 'runtime': runtime}
        self.results.append(result)
        
        
    def use_zip(self):
        '''Use zip function to iterate'''
        start = datetime.utcnow()
        
        for (a, b, c) in zip(self.df.a, self.df.b, self.df.c):
            c = add(a, b)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        
        result = {'loop': 'zip', 'runtime': runtime}
        self.results.append(result)
        
        
    def use_apply(self):
        '''Use apply function to iterate'''
        start = datetime.utcnow()
        
        self.df.c = self.df.apply(lambda x: add(x['a'], x['b']), axis=1)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        
        result = {'loop': 'apply', 'runtime': runtime}
        self.results.append(result)
        
        
    def use_map(self):
        '''Use apply function to iterate'''
        start = datetime.utcnow()
        
        self.df.c = self.df.map(lambda x: add(x['a'], x['b']), axis=1)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        
        result = {'loop': 'map', 'runtime': runtime}
        self.results.append(result)
        
        
    def use_pandas(self):
        '''Use pandas to iterate'''
        start = datetime.utcnow()
        
        self.df.c = add(self.df['a'], self.df['b'])
        
        runtime = (datetime.utcnow() - start).total_seconds()
        
        result = {'loop': 'pandas', 'runtime': runtime}
        self.results.append(result)
        
        
    def use_numpy(self):
        '''Use numpy to iterate'''
        start = datetime.utcnow()
        
        self.df.c = add(self.df.b.values, self.df.a.values)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        
        result = {'loop': 'numpy', 'runtime': runtime}
        self.results.append(result)
        
        
    def use_iterrows(self):
        '''Use iterrows to iterate'''
        start = datetime.utcnow()
        
        
        for index, row in self.df.iterrows():
            row.c = add(row.a, row.b)
        #self.df.c = df.iterrows(add(a, b))
        
        runtime = (datetime.utcnow() - start).total_seconds()
        
        result = {'rows': self.df.size, 'loop': 'iterrows', 'runtime': runtime}
        self.results.append(result)


    def use_itertuples(self):
        '''Use iterrows to iterate'''
        start = datetime.utcnow()
        
        
        for row in self.df.itertuples():
            self.df.c.iloc[row.Index] = add(row.a, row.b)
        #self.df.c = df.iterrows(add(a, b))
        
        runtime = (datetime.utcnow() - start).total_seconds()
        
        result = {'loop': 'itertuples', 'runtime': runtime}
        self.results.append(result)
        
        
    
        
        
if __name__ == '__main__':
    size = [10, 100, 1000, 10000]
    l = Loop()
    
    for i in size:
        l.set_df(i)
        l.use_for()
        l.use_list_comprehension()
        l.use_while()
        l.use_zip()
        l.use_apply()
        l.use_numpy()
        l.use_pandas()
        l.use_iterrows()
        l.use_itertuples()
        d = asdict(l)
        print(json.dumps(d['result'], indent=2))
