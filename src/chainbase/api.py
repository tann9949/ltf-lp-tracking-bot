import os
import time
from typing import Dict, List

import requests

from ..constant import Chain


class ChainBaseAPI(object):
    
    def __init__(self) -> None:
        self.apikey = os.getenv("CHAINBASE_APIKEY", None)
        
        if self.apikey is None:
            raise Exception(f"No Chainbase API key detected!")
        
    def __get_header(self) -> Dict[str, str]:
        return {
            "x-api-key": self.apikey,
            "accept": "application/json"
        }
        
    @staticmethod
    def get_network_id(chain: Chain) -> int:
        """Source from https://docs.chainbase.com/reference/supported-chains"""
        if chain == Chain.POLYGON:
            return 137
        elif chain == Chain.ARBITRUM_ONE:
            return 42161
        elif chain == Chain.OPTIMISM:
            return 10
        elif chain == Chain.BASE:
            return 8453
        else:
            raise ValueError(f"Chain {chain} not supported")
        
    def get_holders(self, chain: Chain, address: str) -> List[str]:
        network_id = ChainBaseAPI.get_network_id(chain)
        
        page_id = 1
        token_holders = []
        
        url = f"https://api.chainbase.online/v1/token/holders?chain_id={network_id}&contract_address={address}"
        r = requests.get(url+f"&page={page_id}&limit=100", headers=self.__get_header())
        r = r.json()
        
        if "error" in r:
            raise Exception(f"Error requesting URL:\n{url}\nWith response:\n{r}")
        
        token_holders.extend(r["data"])
        page_id += 1
        time.sleep(1)
        
        while True:
            r = requests.get(url+f"&page={page_id}&limit=100", headers=self.__get_header())
            r = r.json()
            token_holders.extend(r["data"])
            
            if "next_page" not in r:
                assert len(token_holders) == r["count"]
                break
            
            time.sleep(1)
            
            # update
            page_id += 1
            
        return token_holders
            
            
            
        
        
