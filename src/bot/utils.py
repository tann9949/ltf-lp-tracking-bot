import logging
import time
from typing import Any, List

from telegram import Update
from telegram.error import NetworkError

from ..constant import Asset, Chain


async def reply_image(update: Update, img_path: str) -> None:
    if update.message is not None:
        await update.message.reply_photo(
            photo=img_path
        )
    else:
        logging.info("update.message is None!")


async def reply_markdown(update: Update, message: str) -> None:
    if update.message is not None:
        await update.message.reply_markdown_v2(
            text=message
        )
    else:
        logging.info("update.message is None!")


async def reply_message(update: Update, message: str, do_retry: bool = False) -> None:
    if do_retry:
        # send message with retries
        is_sent = False
        while not is_sent:
            try:
                await update.message.reply_text(
                    text=message
                )
            except NetworkError as e:
                logging.info("Error sending message. Retrying in 0.5 seconds")
                logging.debug(e)
                time.sleep(0.5)
                continue
    else:
        # send message without retry
        if update.message is not None:
            await update.message.reply_text(
                text=message
            )
        else:
            logging.info("update.message is None!")
            
            
def get_assets(chain: Chain) -> List[Asset]:
    
    if chain in [Chain.ARBITRUM_ONE, Chain.BNB_CHAIN, Chain.OPTIMISM, Chain.POLYGON, Chain.GNOSIS]:
        return [Asset.USDT, Asset.USDC, Asset.WETH, Asset.DAI]
    elif chain == Chain.LINEA:
        return [Asset.USDT, Asset.USDC, Asset.WETH]
    elif chain == Chain.METIS:
        return [Asset.USDT, Asset.USDC, Asset.WETH, Asset.METIS]
    else:
        raise ValueError(f"Unknown chain {chain}")
    
    
def prettify_chain(chain: Chain):
    if chain == Chain.ARBITRUM_ONE:
        return "Arbitrum One"
    elif chain == Chain.OPTIMISM:
        return "Optimism"
    elif chain == Chain.POLYGON:
        return "Polygon Mainnet"
    elif chain == Chain.BNB_CHAIN:
        return "BNB Chain"
    elif chain == Chain.GNOSIS:
        return "Gnosis Chain"
    elif chain == Chain.LINEA:
        return "Linea"
    elif chain == Chain.METIS:
        return "Metis"
    else:
        raise ValueError(f"Unknown chain {chain}")
