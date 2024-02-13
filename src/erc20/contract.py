from web3 import Web3, HTTPProvider

from .abi import erc20_abi
from ..constant import Asset, Chain
from ..providers import default_providers


class LPNotFoundException(Exception):
    pass


class ERC20(object):

    @staticmethod
    def get_default_provider(chain: Chain) -> Web3:
        return Web3(HTTPProvider(default_providers[chain]))

    def __init__(self, chain: Chain, address: str) -> None:
        self.provider = ERC20.get_default_provider(chain)
        self.chain = chain

        self.address = address \
            if self.provider.is_checksum_address(address) \
            else self.provider.to_checksum_address(address)
        self.contract = self.provider.eth.contract(
            Web3.to_checksum_address(self.address), abi=erc20_abi)
        
        # default values
        self.decimal = self._decimal()
        self.name = self._name()
        self.symbol = self._symbol()
    
    def _name(self) -> str:
        return self.contract.functions.name().call()
    
    def _symbol(self) -> str:
        return self.contract.functions.symbol().call()
    
    def _decimal(self) -> int:
        return self.contract.functions.decimals().call()
    
    def total_supply(self) -> int:
        return self.contract.functions.totalSupply().call()
    
    def balance_of(self, address: str) -> int:
        address = Web3.to_checksum_address(address)
        return self.contract.functions.balanceOf(address).call()
    
    def get_summary(self, address: str) -> dict:
        _balance = self.balance_of(address) / 10**(self.decimal)
        _total_supply = self.total_supply() / 10**(self.decimal)
        return {
            "balance": _balance,
            "pct_total_supply": _balance / _total_supply,
        }
        
    @classmethod
    def get_asset(cls, chain: Chain, asset: Asset) -> "ERC20":
        
        if chain == Chain.ARBITRUM_ONE:
            if asset == Asset.DAI:
                return cls(
                    chain=Chain.ARBITRUM_ONE,
                    address="0x61B3184be0c95324BF00e0DE12765B5f6Cc6b7cA"
                )
            elif asset == Asset.USDC:
                return cls(
                    chain=Chain.ARBITRUM_ONE,
                    address="0xDa492C29D88FfE9B7cbfA6DC068C2f9befaE851b"
                )
            elif asset == Asset.USDT:
                return cls(
                    chain=Chain.ARBITRUM_ONE,
                    address="0x45d0736D77A72AE2Bd3c5770878bd85b72895057"
                )
            elif asset == Asset.WETH:
                return cls(
                    chain=Chain.ARBITRUM_ONE,
                    address="0xb86AF5eB59A8e871bfA573FA656123ea86F47c3a"
                )
            else:
                raise LPNotFoundException(f"Arbitrum One doesn't support {asset} LP")
        elif chain == Chain.OPTIMISM:
            if asset == Asset.WETH:
                return cls(
                    chain=Chain.OPTIMISM,
                    address="0x3C12765d3cFaC132dE161BC6083C886B2Cd94934"
                )
            elif asset == Asset.DAI:
                return cls(
                    chain=Chain.OPTIMISM,
                    address="0xeD6d021DcA3d31D63997e4985fa6Eb3A2B745472"
                )
            elif asset == Asset.USDC:
                return cls(
                    chain=Chain.OPTIMISM,
                    address="0xB12A1Be740B99D845Af98098965af761be6BD7fE"
                )
            elif asset == Asset.USDT:
                return cls(
                    chain=Chain.OPTIMISM,
                    address="0x2C7FA89CC5Ea38d4e5193512b9C10808348Ba74F"
                )
            else:
                raise LPNotFoundException(f"Optimism doesn't support {asset} LP")
        elif chain == Chain.BNB_CHAIN:
            if asset == Asset.WETH:
                return cls(
                    chain=Chain.BNB_CHAIN,
                    address="0x223F6A3B8d087741BF99a2531DC53cd15745eBa7"
                )
            elif asset == Asset.DAI:
                return cls(
                    chain=Chain.BNB_CHAIN,
                    address="0xf9D88D200f3D9B45Bd9f8f3ae124f59a4fbdbae5"
                )
            elif asset == Asset.USDC:
                return cls(
                    chain=Chain.BNB_CHAIN,
                    address="0xc170908481E928DfA39DE3D0d31bEa6292692F8e"
                )
            elif asset == Asset.USDT:
                return cls(
                    chain=Chain.BNB_CHAIN,
                    address="0x9350470389848979fCdFEd28352Ff9e0C9Aa87e9"
                )
            else:
                raise LPNotFoundException(f"BNB Chain doesn't support {asset} LP")
        
        elif chain == Chain.POLYGON:
            if asset == Asset.WETH:
                return cls(
                    chain=Chain.POLYGON,
                    address="0xeF1348dAC70e8349513E4Ae7498F302e27102101"
                )
            elif asset == Asset.DAI:
                return cls(
                    chain=Chain.POLYGON,
                    address="0xe6228819A3416a256DFEF2568A75737046438cB8"
                )
            elif asset == Asset.USDC:
                return cls(
                    chain=Chain.POLYGON,
                    address="0xa03258b76Ef13AF716370529358f6A79eb03ec12"
                )
            elif asset == Asset.USDT:
                return cls(
                    chain=Chain.POLYGON,
                    address="0x7F7948B1345b6A95b65a001278b480CE12cA66E5"
                )
            else:
                raise LPNotFoundException(f"Polygon doesn't support {asset} LP")
        
        elif chain == Chain.GNOSIS:
            
            if asset == Asset.WETH:
                return cls(
                    chain=Chain.GNOSIS,
                    address="0x7aC5bBefAE0459F007891f9Bd245F6beaa91076c"
                )
            elif asset == Asset.DAI:
                return cls(
                    chain=Chain.GNOSIS,
                    address="0x98f7656A6C09388c646ff423ED82980675a152dD"
                )
            elif asset == Asset.USDC:
                return cls(
                    chain=Chain.GNOSIS,
                    address="0xA639FB3f8C52e10E10a8623616484d41765d5F82"
                )
            elif asset == Asset.USDT:
                return cls(
                    chain=Chain.GNOSIS,
                    address="0xD8a772fD2B7872230cCD92EF073bE81De87137D7"
                )
            else:
                raise LPNotFoundException(f"Gnosis doesn't support {asset} LP")
        
        elif chain == Chain.LINEA:
            
            if asset == Asset.WETH:
                return cls(
                    chain=Chain.LINEA,
                    address="0x611C91C807c07B4D358224Fb5Dcd3999f36167B3"
                )
            elif asset == Asset.USDC:
                return cls(
                    chain=Chain.LINEA,
                    address="0x66bE8926aa5cbDF24f07560d36999bF9B6B2Bb87"
                )
            elif asset == Asset.USDT:
                return cls(
                    chain=Chain.LINEA,
                    address="0xFB8A9F8b13A6D297A1478aF67bDE98362BE532D6"
                )
            else:
                raise LPNotFoundException(f"Gnosis doesn't support {asset} LP")
        
        elif chain == Chain.METIS:
            if asset == Asset.WETH:
                return cls(
                    chain=Chain.METIS,
                    address="0x5C70a3ae965cf94ee94b77E620bA425DA33EC187"
                )
                
            elif asset == Asset.USDC:
                return cls(
                    chain=Chain.METIS,
                    address="0x02e226Ed4Ab684Ba421922aa68Af68a7733deadd"
                )
                
            elif asset == Asset.USDT:
                return cls(
                    chain=Chain.METIS,
                    address="0x5f0d5D93F8F3711B5dEba819F824F37675E73Dc2"
                )
                
            elif asset == Asset.METIS:
                return cls(
                    chain=Chain.METIS,
                    address="0xb0419750997c2c9f5e0C5C6d4eb89CFeFB7ca84F"
                )
            else:
                raise LPNotFoundException(f"Metis doesn't support {asset} LP")
        
        else:
            raise ValueError(f"Unknown chain {chain}")
        