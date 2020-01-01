# data analysis/wrangling
import pandas as pd
import numpy as np
# data modeling
from dataclasses import dataclass, asdict, field
from typing import Dict
 
@dataclass
class Loop:
    df: pd.DataFrame = None
    results: Dict = None
    
    
    def create_df(self):
        ''''''
        try:
            # Load pickled pandas object from file
            df = pd.read_pickle('df.pkl')
        
        except Exception as e:
            # Create dataframe with random numbers
            df = pd.DataFrame(np.random.randint(0, 1000, size=(40000, 2)), columns=list('ab'))
            # Pickle pandas object
            df.to_pickle('df.pkl')
            
        finally:
            print(df)
            self.df = df
            
        
    
    def __post_init__(self):
        # if pickled dataframe exists load it, otherwise create a new dataframe and pickle it
        pass
        
        
 
 
 
if __name__ == '__main__':
    l = Loop()
    l.create_df()
    print(asdict(l))
