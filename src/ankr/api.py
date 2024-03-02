import logging
import os
from typing import Any, Dict, List, Optional

import requests

from ..erc20 import ERC20
from ..constant import Chain, Asset


class AnkrAPI(object):
    
    def __init__(self) -> None:
        if self.__get_key() is None:
            raise Exception(f"Ankr URL not found!")
        
    def __get_key(self) -> Optional[str]:
        return os.getenv("ANKR_KEY", None)
    
    @staticmethod
    def resolve_chain(chain: Chain) -> str:
        if chain == Chain.OPTIMISM:
            return "optimism"
        else:
            raise ValueError(f"chain {chain} doesn't supported yet.")
    
    def get_token_holders_and_balance(self, chain: Chain, address: str, min_filter: float = 0.) -> Dict[str, float]:
        # initialize
        holders = dict()
        
        # create headers
        headers = {
            "Content-Type": "application/json"
        }
        
        # create json body
        body = {
            "jsonrpc": "2.0",
            "method": "ankr_getTokenHolders",
            "params": {
                "blockchain": AnkrAPI.resolve_chain(chain),
                "contractAddress": address,
                "pageSize": 10000
            },
            "id": 1
        }
        
        # request API
        url = f"https://rpc.ankr.com/multichain/{self.__get_key()}"
        r = requests.post(
            url,
            headers=headers,
            json=body
        )
        
        # raise exception if fail
        if r.status_code != 200:
            raise Exception(r.text)
        
        # parse to dict and get result
        r = r.json()["result"]
        
        # store for later sanity check
        holder_counts = r["holdersCount"]
        
        # update holders
        for _holder in r["holders"]:
            _balance = float(_holder["balance"])
            _address = _holder["holderAddress"]
            
            assert _address not in holders
            holders[_address] = _balance
        
        # reiterate if one page isn't enough
        while len(r["nextPageToken"]) > 0:
            # create headers
            headers = {
                "Content-Type": "application/json"
            }
            
            # create json body
            body = {
                "jsonrpc": "2.0",
                "method": "ankr_getTokenHolders",
                "params": {
                    "blockchain": AnkrAPI.resolve_chain(chain),
                    "contractAddress": address,
                    "pageSize": 10000,
                    "pageToken": r["nextPageToken"]
                },
                "id": 1
            }
            
            # request API
            r = requests.post(
                f"https://rpc.ankr.com/multichain/{self.__get_key()}",
                headers=headers,
                json=body
            )
            
            # raise exception if fail
            if r.status_code != 200:
                raise Exception(r.text)
            
            # parse to dict
            r = r.json()["result"]
            
            # update holders
            for _holder in r["holders"]:
                _balance = float(_holder["balance"])
                _address = _holder["holderAddress"]
                
                if float(_balance) < min_filter:
                    logging.debug(f"Removing {_address} (balance {_balance} < {min_filter})")
                    continue
                
                assert _address not in holders, f"Address {_address} duplicated"
                holders[_address] = _balance
            
        assert len(holders) == holder_counts, f"Total number of holders conflict. Expect {holder_counts} but got {len(holders)}"
        
        # filtering
        filtered_holders = {
            _address: _balance
            for _address, _balance in holders.items()
            if _balance >= min_filter
        }
        logging.info(f"Holders filtered from {len(holders)} -> {len(filtered_holders)}")
        
        return filtered_holders
    
    def get_lp_holders_and_balance(self, chain: Chain, asset: Asset, min_filter: float = 0.) -> List[Dict[str, str]]:
        contract_address = ERC20.get_asset(chain, asset).address
        holders = self.get_token_holders_and_balance(chain, contract_address, min_filter=min_filter)
        
        return holders