import os

from moralis import evm_api


class MoralisAPI(object):
    
    def __init__(self) -> None:
        if self.__get_apikey() is None:
            raise Exception(f"Moralis API key not found!")
    
    def __get_apikey(self) -> str:
        return os.getenv("MORALIS_APIKEY", None)
    
    def get_holders(self):
        evm_api