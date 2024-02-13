import asyncio
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import List

from web3 import Web3, HTTPProvider

from .abi import nft_abi
from ..constant import Asset, Chain
from ..providers import default_providers


class LPNotFoundException(Exception):
    pass


class SpecialNFTContract(object):

    @staticmethod
    def get_default_provider(chain: Chain) -> Web3:
        return Web3(HTTPProvider(default_providers[chain]))
    
    def __init__(self) -> None:
        # constant for special NFT
        # https://arbiscan.io/address/0xC88a0B7BCB32283a2B2Fc00aD3DF234eA4a8e6E5
        chain = Chain.ARBITRUM_ONE # special NFT was in Arbitrum
        address = "0xC88a0B7BCB32283a2B2Fc00aD3DF234eA4a8e6E5"
        
        self.provider = SpecialNFTContract.get_default_provider(chain)
        self.chain = chain

        self.address = address \
            if self.provider.is_checksum_address(address) \
            else self.provider.to_checksum_address(address)
        self.contract = self.provider.eth.contract(
            Web3.to_checksum_address(self.address), abi=nft_abi)
        
        # default values
        self.symbol = self._symbol()
        self.name = self._name()
        
        # sanity check if NFT was correct address
        assert self.symbol == "CONNEXTRARE LP NFT"
        assert self.name == "Connext Rare LP NFT"
        
    def _name(self) -> str:
        return self.contract.functions.name().call()
    
    def _symbol(self) -> str:
        return self.contract.functions.symbol().call()
    
    def total_supply(self) -> int:
        return self.contract.functions.totalSupply().call()
    
    def balance_of(self, address: str) -> int:
        address = Web3.to_checksum_address(address)
        return self.contract.functions.balanceOf(address).call()
    