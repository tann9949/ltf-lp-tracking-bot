import asyncio
import os

from covalent import CovalentClient

from ..erc20.contract import ERC20
from ..constant import Asset, Chain


class CovalentAPI(object):
    
    def __init__(self) -> None:
        api_key = os.getenv("COVALENT_APIKEY", None)
        if api_key is None:
            raise Exception(f"API Key for covalent not found!")
        
        self.client = CovalentClient(api_key)
        
    @staticmethod
    def resolve_chain_name(chain: Chain) -> str:
        if chain == Chain.OPTIMISM:
            return "optimism-mainnet"
        elif chain == Chain.ARBITRUM_ONE:
            return "arbitrum-mainnet"
        elif chain == Chain.BNB_CHAIN:
            return "bsc-mainnet"
        elif chain == Chain.POLYGON:
            return "matic-mainnet"
        else:
            raise ValueError(f"Unsupported chain: {chain}")

    async def get_token_balances_for_wallet_address(self, **kwargs):
        output = list()
        async for res in self.client.balance_service.get_token_holders_v2_for_token_address(**kwargs):
            output.append(res)
            
        return output
        
    def get_holders(self, chain: Chain, asset: Asset) -> dict:
        chain_name = CovalentAPI.resolve_chain_name(chain)
        
        lp_token = ERC20.get_asset(chain, asset)
        return asyncio.run(self.get_token_balances_for_wallet_address(
            chain_name=chain_name,
            token_address=lp_token.address,
            page_size=1000
        ))