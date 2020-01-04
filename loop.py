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
# decorators
from functools import wraps


def add(a, b):
    '''Calculate the sum of two integers'''
    #return (a + b) * 2
    return a**2 + b**2
    

@dataclass
class Result:
    size: int
    loop_type: str
    execution_time: int = field(default=None)
    
 
@dataclass
class Loop:
    size: int = field(default=None, repr=False)
    df: pd.DataFrame = field(default=None, repr=False)
    #results: List = field(default_factory=list)
    result: List[Result] = field(default_factory=list, repr=False)
    
    
    def _timeit(func):
        '''Decorator hidden from the class methods'''                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
        def wrapper(self, *args, **kwargs):
            start = datetime.utcnow()
            result = func(self, *args, **kwargs)
            result['execution_time'] = (datetime.utcnow() - start).total_seconds()
            self.result.append(result)
        return wrapper
        
        
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
            self.df = df
            self.size = size
            
            
    @_timeit        
    def use_list_comprehension(self, loop):
        [add(a, b) for a, b in zip(self.df.a, self.df.b)]
        return asdict(Result(self.size, loop))
        
    
    @_timeit 
    def use_for(self, loop):
        for i in range(0, len(self.df)):
            add(self.df.a.iloc[i], self.df.a.iloc[i])
        return asdict(Result(self.size, loop))
        
    
    @_timeit     
    def use_while(self, loop):
        i = len(self.df) - 1
        while i != 0:
            add(self.df.a.iloc[i], self.df.a.iloc[i])
            i = i - 1
        return asdict(Result(self.size, loop))
        
    
    @_timeit     
    def use_zip(self, loop):
        for (a, b) in zip(self.df.a, self.df.b):
            add(a, b)
        return asdict(Result(self.size, loop))
        
    
    @_timeit 
    def use_apply(self, loop):   
        self.df.apply(lambda x: add(x['a'], x['b']), axis=1)
        return asdict(Result(self.size, loop))
        
    
    @_timeit 
    def use_map(self, loop):
        self.df.map(lambda x: add(x['a'], x['b']), axis=1)
        return asdict(Result(self.size, loop))
        
    
    @_timeit 
    def use_pandas(self, loop):
        add(self.df['a'], self.df['b'])
        return asdict(Result(self.size, loop))
        
    
    @_timeit 
    def use_numpy(self, loop):
        add(self.df.b.values, self.df.a.values)
        return asdict(Result(self.size, loop))
        
    
    @_timeit     
    def use_iterrows(self, loop):
        for index, row in self.df.iterrows():
            add(row.a, row.b)
        return asdict(Result(self.size, loop))

    
    @_timeit 
    def use_itertuples(self, loop):
        for row in self.df.itertuples():
            add(row.a, row.b)
        return asdict(Result(self.size, loop))
        
    
    @_timeit 
    def use_iter_while(self, loop):
        iterator = iter(self.df.values)
        stop_loop = False
        
        while not stop_loop:
            try: 
                iterator = next(iterator)
                add(iterator[0], iterator[1])
            
            except:
                stop_loop = True

        return asdict(Result(self.size, loop))
        
    
# Execute when script is run as main module       
if __name__ == '__main__':
    sizes = [10, 100, 1000]
    iterators = ['for', 'list_comprehension', 'while', 'zip', 'apply', 'pandas', 'numpy', 'iterrows', 'itertuples', 'iter_while'] 

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
