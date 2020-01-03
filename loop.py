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
from timeme import timeme
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
    size: int = field(default=None, repr=False)
    df: pd.DataFrame = field(default=None, repr=False)
    results: List = field(default_factory=list)
    result: List[Result] = field(default_factory=list, repr=False)
    
    @timeme
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
            df['c'] = 0
            
        finally:
            self.df = df
            self.size = size
            
            
    def use_list_comprehension(self, loop):
        '''Use for loop to iterate'''
        start = datetime.utcnow()
        c = [add(a, b) for a, b in zip(self.df.a, self.df.b)]
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
            
    
    def use_for(self, loop):
        '''Use for loop to iterate'''
        start = datetime.utcnow()
        for i in range(0, len(self.df)):
            self.df.c.iloc[i] = add(self.df.a.iloc[i], self.df.a.iloc[i])
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
        
        
    def use_while(self, loop):
        '''Use while loop to iterate'''
        start = datetime.utcnow()
        
        i = len(self.df) - 1
        
        while i != 0:
            self.df.c.iloc[i] = add(self.df.a.iloc[i], self.df.a.iloc[i])
            i = i - 1
            
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
        
        
    def use_zip(self, loop):
        '''Use zip function to iterate'''
        start = datetime.utcnow()
        
        for (a, b, c) in zip(self.df.a, self.df.b, self.df.c):
            c = add(a, b)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
        
        
    def use_apply(self, loop):
        '''Use apply function to iterate'''
        start = datetime.utcnow()
        
        self.df.c = self.df.apply(lambda x: add(x['a'], x['b']), axis=1)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
        
        
    def use_map(self, loop):
        '''Use apply function to iterate'''
        start = datetime.utcnow()
        
        self.df.c = self.df.map(lambda x: add(x['a'], x['b']), axis=1)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
        
        
    def use_pandas(self, loop):
        '''Use pandas to iterate'''
        start = datetime.utcnow()
        
        self.df.c = add(self.df['a'], self.df['b'])
        
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
        
        
    def use_numpy(self, loop):
        '''Use numpy to iterate'''
        start = datetime.utcnow()
        
        self.df.c = add(self.df.b.values, self.df.a.values)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
        
        
    def use_iterrows(self, loop):
        '''Use iterrows to iterate'''
        start = datetime.utcnow()
        
        for index, row in self.df.iterrows():
            row.c = add(row.a, row.b)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)


    def use_itertuples(self, loop):
        '''Use iterrows to iterate'''
        start = datetime.utcnow()
        
        for row in self.df.itertuples():
            self.df.c.iloc[row.Index] = add(row.a, row.b)
        #self.df.c = df.iterrows(add(a, b))
        
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
        
        
    def use_iter_while(self, loop):
        '''Use iter and custom while loop'''
        start = datetime.utcnow()
        
        for row in self.df.itertuples():
            pass
        runtime = (datetime.utcnow() - start).total_seconds()
        result = asdict(Result(self.size, loop, runtime))
        self.result.append(result)
        

# Execute when script is run from CLI        
if __name__ == '__main__':
    sizes = [10, 100, 1000]
    iterators = ['for', 'list_comprehension', 'while', 'zip', 'apply', 'pandas', 'iterrows', 'itertuples']#, 'iter_while']
    # Initialize a Loop object
    l = Loop()
    
    for size in sizes:
        l.set_df(size)
        
        for iterator in iterators:
            # Evaluate Loop functions
            loop = 'l.use_{}("{}")'.format(iterator, iterator)
            eval(loop)
    
    # Save results to JSON
    with open('data.json', 'w') as file:
        json.dump(l.result, file, indent=2)
    print(json.dumps(l.result, indent=2))
