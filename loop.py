# data analysis/wrangling
import pandas as pd
import numpy as np
import json
# data modeling
from dataclasses import dataclass, asdict, field
from typing import Dict, List
# loop progress
from tqdm import trange, tqdm
# date
from datetime import datetime


def add(a, b):
    '''Calculate the sum of two integers'''
    return a + b

def pythagorean(a, b):
    '''Calculate the hypotnuse of a triangle given the lengths of side a, b'''
    return sqrt(a**2 + b**2)



 
@dataclass
class Loop:
    df: pd.DataFrame = None
    results: List = field(default_factory=list)
    
    
    def __post_init__(self):
        '''Create or load pickled dataframe '''
        try:
            # Load pickled pandas object from file
            df = pd.read_pickle('df.pkl')
        
        except Exception as e:
            # Create dataframe with random numbers
            df = pd.DataFrame(np.random.randint(0, 1000, size=(400000, 2)), columns=list('ab'))
            # Pickle pandas object
            df.to_pickle('df.pkl')
            
        finally:
            df['c'] = 0
            self.df = df
            
    
    def use_standard(self):
        '''Utilize the standard python for loop to iterate over the rows of the df'''
        start = datetime.utcnow()
        for i in range(0, len(self.df)):
            self.df.c.iloc[i] = add(self.df.a.iloc[i], self.df.a.iloc[i])
        runtime = (datetime.utcnow() - start).total_seconds()
        self.results.append({'loop': 'standard', 'runtime': runtime})
        
        
    def use_zip(self):
        ''''''
        start = datetime.utcnow()
        for (a, b, c) in zip(self.df.a, self.df.b, self.df.c):
            c = add(a, b)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        self.results.append({'loop': 'zip', 'runtime': runtime})
        
        
    def use_apply(self):
        ''''''
        start = datetime.utcnow()
        self.df.c = self.df.apply(lambda x: add(x['a'], x['b']), axis=1)
        
        runtime = (datetime.utcnow() - start).total_seconds()
        self.results.append({'loop': 'apply', 'runtime': runtime})
        
        
if __name__ == '__main__':
    l = Loop()
    #l.create_df()
    l.use_standard()
    l.use_zip()
    l.use_apply()
    print(json.dumps(l.results, indent=2))
