"""Create csv files that records USDT, USDC, ETH holders
on Optimism. The script took around 5-10 minutes to run.
"""
import logging
import os
import time
from argparse import ArgumentParser, Namespace
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Union, Optional

import boto3
import pandas as pd
from dotenv import load_dotenv

from src.ankr.api import AnkrAPI
from src.bot.utils import get_assets
from src.constant import Asset, Chain
from src.special_nft.contract import SpecialNFTContract

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def run_parser() -> Namespace:
    parser = ArgumentParser()
    
    parser.add_argument("-c", "--chain", type=str, default=Chain.OPTIMISM, help="Chain to get asset")
    parser.add_argument("-a", "--assets", type=str, default="weth,usdc,usdt", help="Assets to get balance")
    parser.add_argument("--usd-filter", type=float, default=10.0, help="Minimum USDT or USDC LP holdings")
    parser.add_argument("--weth-filter", type=float, default=0.1, help="Minimum WETH LP holdings")
    # save params
    parser.add_argument("--save-method", type=str, choices=["local", "s3"], default="local", help="Assets to get balance")
    parser.add_argument("--s3-bucket", type=str, help="Name of the S3 bucket to upload the file")
    
    return parser.parse_args()


def get_special_nft_status(holders: Dict[str, float]) -> Dict[str, Union[float, bool]]:
    wallet_dict = dict()
    
    # multiprocess for fast API request
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_call = {
            executor.submit(
                lambda _address, _balance: (_balance, SpecialNFTContract().balance_of(_address) > 0.), 
                address, balance
            ): (address, balance) 
            for address, balance in holders.items()
        }

        for future in as_completed(future_to_call):
            _address, _ = future_to_call[future]
            try:
                result = future.result()
                if result:
                    _balance, is_special = result
                    wallet_dict[_address] = (_balance, is_special)
            except Exception as exc:
                logging.error(exc)
        
    return wallet_dict


def dict_to_df(balance_dict: Dict) -> pd.DataFrame:
    df = pd.DataFrame([
        (_wallet, _balance, _special)
        for _wallet, (_balance, _special) in balance_dict.items()
    ], columns=["wallet", "balance", "is_special_nft"])
    
    df = df.sort_values("balance", ascending=False)
    
    return df


def upload_to_s3(file_path: str, bucket: str, key: Optional[str] = None) -> None:
    logging.info(f"Pushing {file_path} to S3")
    key = os.path.basename(file_path) if key is None else key
    
    s3_client = boto3.client("s3")
    s3_client.upload_file(file_path, bucket, key)
    logging.info("File push to S3 successfully")


def main(args: Namespace) -> None:
    global_st = time.time()
    
    if load_dotenv():
        logging.info(f".env loaded!")
        
    # unpack args
    chain = args.chain
    assets = args.assets.split(",")
    usd_filter = args.usd_filter
    weth_filter = args.weth_filter
    save_method = args.save_method
    s3_bucket = args.s3_bucket if save_method == "s3" else None
    
    # sanity check
    if save_method == "s3" and s3_bucket is None:
        raise Exception("S3 saving requires bucket specification")
    
    # sanity asset/chain check
    assert chain in [Chain.OPTIMISM], f"{chain} not supported"
    all_assets = get_assets(chain)
    assert all(_a in all_assets for _a in assets), f"Invalid asset: {assets}"
    
    # initialize API
    api = AnkrAPI()
    
    for _asset in assets:
        min_filter = 0.
        
        if _asset in [Asset.USDC, Asset.USDT, Asset.DAI]:
            logging.info(f"Setting min filter of {_asset} LP to {usd_filter}")
            min_filter = usd_filter
        elif _asset in [Asset.WETH]:
            logging.info(f"Setting min filter of {_asset} LP to {weth_filter}")
            min_filter = weth_filter
        else:
            logging.warning(f"Unknown asset, not setting minimum filter")
        
        st = time.time()
        logging.info(f"Getting holders/balance for Connext {_asset} LP")
        holders = api.get_lp_holders_and_balance(chain, _asset, min_filter=min_filter)
        logging.info(f"All holders retrieved. Took {time.time() - st:.2f} seconds")
        
        # get special NFT status
        holders_with_special_status = get_special_nft_status(holders)

        # write results
        df = dict_to_df(holders_with_special_status)
        
        # save to local
        output_path = f"outputs/{chain}_{_asset}_holder_balance.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        
        # push to s3 if needed
        if save_method == "s3":
            upload_to_s3(output_path, s3_bucket)
        
        logging.info(f"Holder statistics for was saved to {output_path}")
        
    logging.info(f"Process finished in {time.time() - global_st:.2f} seconds")


if __name__ == "__main__":
    args = run_parser()
    main(args)
