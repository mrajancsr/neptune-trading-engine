from typing import List, TypeVar
import pandas as pd


Exchange = TypeVar("Exchange")
SWAP = 'swap'
SPOT = 'spot'
ID = 'id'

def get_market_symbols(exchg: Exchange) -> List[str]:
    markets_df = pd.DataFrame(list(exchg.load_markets().values()))
    perps = markets_df[markets_df.type == SWAP]
    perps = perps[ID].to_list()

    spots = markets_df[markets_df.type == SPOT]
    spots = spots[ID].to_list()

    return perps + spots