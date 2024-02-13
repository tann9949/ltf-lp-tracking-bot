from .constant import Chain

# from https://chainlist.org/
default_providers = {
    Chain.OPTIMISM: "https://1rpc.io/op",
    Chain.ARBITRUM_ONE: "https://1rpc.io/arb",
    Chain.BNB_CHAIN: "https://bscrpc.com",
    Chain.GNOSIS: "https://1rpc.io/gnosis",
    Chain.POLYGON: "https://polygon.blockpi.network/v1/rpc/public",
    Chain.LINEA: "https://linea.blockpi.network/v1/rpc/public",
    Chain.METIS: "https://andromeda.metis.io/?owner=1088"
}