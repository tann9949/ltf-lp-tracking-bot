"""Create csv files that records USDT, USDC, ETH holders
on Optimism. The script took around 5-10 minutes to run.
"""
import logging
import os
import time
from argparse import ArgumentParser, Namespace
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

import boto3
import pandas as pd
from dotenv import load_dotenv

from src.ankr.api import AnkrAPI
from src.bot.utils import get_assets
from src.constant import Asset, Chain
from src.price import get_weth_price
from src.special_nft.contract import SpecialNFTContract

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def run_parser() -> Namespace:
    parser = ArgumentParser()

    # asset params
    parser.add_argument("-c", "--chain", type=str, default=Chain.OPTIMISM, help="Chain to get asset")
    parser.add_argument("-a", "--assets", type=str, default="weth,usdc,usdt", help="Assets to get balance")
    # price params
    parser.add_argument("--eth-ma-window", type=int, default=7, help="Moving average window for calculating ETH price")
    parser.add_argument("--usd-filter", type=float, default=100.0, help="Minimum USDT or USDC LP holdings")
    # reward params
    parser.add_argument("--reward-amount", type=float, default=2500, help="Total amount of reward to be distributed")
    # save params
    parser.add_argument("--save-method", type=str, choices=["local", "s3"], default="local", help="Assets to get balance")
    parser.add_argument("--s3-bucket", type=str, help="Name of the S3 bucket to upload the file")
    
    return parser.parse_args()        


def dict_to_df(balance_dict: Dict) -> pd.DataFrame:
    df = pd.DataFrame([
        (_wallet, _balance)
        for _wallet, _balance in balance_dict.items()
    ], columns=["wallet", "balance"])
    
    df = df.sort_values("balance", ascending=False)
    
    return df


def resolve_holders_usd(df: pd.DataFrame, eth_ma_window: Optional[int] = None) -> pd.DataFrame:
    # if eth_ma_window is not provided, use fix 1 USD value
    asset_price = 1. if eth_ma_window is None else get_weth_price(ma_days=eth_ma_window)
    
    logging.info(f"Using asset price of {asset_price}")
    
    df["usd_value"] = df["balance"].map(lambda x: x * asset_price)
    return df


def get_special_nft_status(df: pd.DataFrame) -> pd.DataFrame:
    
    def check_nft_status(row):
        try:
            is_special = SpecialNFTContract().balance_of(row["wallet"]) > 0
            return row["wallet"], row["balance"], row["usd_value"], is_special
        except Exception as exc:
            logging.error(exc)
            return row["wallet"], row["balance"], row["usd_value"], False

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(check_nft_status, row): row for _, row in df.iterrows()}

        results = []
        for future in as_completed(futures):
            result = future.result()
            if result:
                results.append(result)

    # Creating a new DataFrame from the results
    new_df = pd.DataFrame(results, columns=["wallet", "balance", "usd_value", "is_special"])
    return new_df


def calculate_reward(df: pd.DataFrame, reward_amt: int) -> pd.DataFrame:
    
    # weight score by usd_value
    df["weighted_score"] = df["usd_value"] / sum(df["usd_value"])
    
    # calculate reward
    df["reward_without_boost"] = df["weighted_score"] * reward_amt
    
    # make sure total reward don't exceed reward_amt
    assert sum(df["reward_without_boost"]) <= reward_amt
    
    # calculate boosted reward if is_special
    # boost by 10%
    df["OP_rewards"] = df["reward_without_boost"] * 1.1 * df["is_special"] + df["reward_without_boost"] * ~df["is_special"]
    
    return df


def upload_to_s3(file_path: str, bucket: str, key: Optional[str] = None, public: bool = True) -> None:
    logging.info(f"Pushing {file_path} to S3")
    key = os.path.basename(file_path) if key is None else key
    
    # set ACL to public-read
    extra_args = {"ACL": "public-read"} if public else {}
    
    s3_client = boto3.client("s3")
    s3_client.upload_file(file_path, bucket, key, ExtraArgs=extra_args)
    logging.info("File push to S3 successfully")


def main(args: Namespace) -> None:
    global_st = time.time()
    
    if load_dotenv():
        logging.info(f".env loaded!")
        
    # unpack args
    chain = args.chain
    assets = args.assets.split(",")
    
    eth_ma_window = args.eth_ma_window
    usd_filter = args.usd_filter
    
    reward_amt = args.reward_amount
    
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
    
    # calculate reward distribution
    reward_per_asset = reward_amt / len(assets)
    logging.info(f"There're {len(assets)} with a total reward of {reward_amt}")
    logging.info(f"Each asset will be allocated reward for {reward_amt}")
    
    for _asset in assets:
        
        st = time.time()
        logging.info(f"Getting holders/balance for Connext {_asset} LP")
        holders = api.get_lp_holders_and_balance(chain, _asset)
        logging.info(f"All holders retrieved. Took {time.time() - st:.2f} seconds")
        
        # convert to dataframe
        df = dict_to_df(holders)
        
        # resolve and filter USD price
        df = resolve_holders_usd(
            df=df, 
            eth_ma_window=eth_ma_window if _asset == Asset.WETH else None
        )
        
        # apply filter
        df = df[df["usd_value"] >= usd_filter]
        
        # get special NFT status
        df = get_special_nft_status(df)
        
        # sort by usd value
        df = df.sort_values("usd_value", ascending=False)
        
        # calculate rewards
        df = calculate_reward(df, reward_amt=reward_per_asset)
        
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
