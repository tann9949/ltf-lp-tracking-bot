from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

from telegram import Update
from telegram.ext import (Application, CallbackContext, CommandHandler,
                          MessageHandler, filters)

from .utils import get_assets, prettify_chain
from ..bot.utils import reply_markdown, reply_message
from ..constant import Chain, Asset
from ..erc20 import ERC20
from ..price import get_asset_price

CHAINS = [
    Chain.ARBITRUM_ONE,
    Chain.BNB_CHAIN,
    Chain.OPTIMISM,
    Chain.GNOSIS,
    Chain.POLYGON,
    Chain.LINEA,
    Chain.METIS
]


def get_lp_summary(chain, asset, wallet):
    lp_token = ERC20.get_asset(chain, asset)
    summary = lp_token.get_summary(wallet)

    if summary["balance"] <= 0.:
        logging.info(f"Wallet {wallet} doesn't hold any {asset} LP on {chain}")
        return None

    return asset, summary


def get_lp_balances(wallet: str) -> dict:
    wallet_dict = dict()

    # multiprocess for fast API request
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_call = {
            executor.submit(get_lp_summary, _chain, _asset, wallet): (_chain, _asset) 
            for _chain in CHAINS for _asset in get_assets(_chain)
        }

        for future in as_completed(future_to_call):
            _chain, _asset = future_to_call[future]
            try:
                result = future.result()
                if result:
                    asset, summary = result
                    wallet_dict.setdefault(_chain, {})[asset] = summary
            except Exception as exc:
                logging.error(f'{_chain} {_asset} generated an exception: {exc}')
        
    return wallet_dict


def format_dict(wallet: str, lp_balances: dict) -> str:
    """
    Wallet: 0x...

    [Arbitrum]
    - 1,888.23 CUSDTLP (0.02% of total supply)
    - 3 CWETHLP (7.24% of total supply)

    Total LP Value: 7,234.22 USD

    [Metis]
    - 7,223.11 CUSDCLP (44.23% of total supply)

    Total LP Value: 7,234.22 USD

    > Total LP balance across all chain in USD: 13,234.23 USD
    """ 
    template = f"Wallet: `{wallet}`\n\n"
    
    weth_price = get_asset_price(Asset.WETH)
    metis_price = get_asset_price(Asset.METIS)

    logging.info(f"WETH price: {weth_price}")
    logging.info(f"METIS price: {metis_price}")

    total_lp_price = 0.
    for _chain in lp_balances:
        template += f"> {prettify_chain(_chain)}\n"
        
        chain_lp_price = 0.
        for _asset in lp_balances[_chain]:
            _bal = lp_balances[_chain][_asset]["balance"]
            _pct_supply = lp_balances[_chain][_asset]["pct_total_supply"]*100
            
            if _asset == Asset.WETH:
                _price = weth_price
            elif _asset == Asset.METIS:
                _price = metis_price
            else:
                _price = get_asset_price(_asset)
            
            chain_lp_price += _price * _bal
            template += f"\- `{_bal:,.8f} C{_asset.upper()}LP` \(`{_pct_supply:.6f}%` of total supply\)\n"
        template += f"\n_Total LP Value: `{chain_lp_price:,.4f} USD`_\n\n"
        
        total_lp_price += chain_lp_price
        
    template += f"> Total LP balance across all chain in USD: `{total_lp_price:,.4f} USD`"
            
    return template.strip()\
        .replace(".", "\.")


class LTFLPBalanceBot(object):

    def __init__(
        self, 
    ) -> None:
        self.app = Application.builder().token(
            token=os.getenv("TELEGRAM_BOT_TOKEN")
        ).build()
        self.add_default_handler()
        self.add_command_handler("start", LTFLPBalanceBot.start_callback)
        self.add_command_handler("help", LTFLPBalanceBot.start_callback)
        self.add_command_handler("lp", LTFLPBalanceBot.lp_balance_callback)
    
    #### bot callback functions ####

    async def start_callback(update: Update, context: CallbackContext) -> None:
        """Send a message when the command /start is issued."""
        logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Default callback triggered")
        
        template = (
            "GM\n"
            "This bot🤖 is a part of "
            "[Connext's Liquidity Task Force](https://forum.connext.network/t/closed-rfc-liquidity-task-force/906) "
            "\(LTF\) where it's role is to "
            "fetch LP balance of a wallet across all chains.\n\n"
            "Here're the commands you can use on the bot:\n"
            "\- `/lp <wallet>`: Get the LP address across all chains\n"
        ).replace(".", "\.")
        await reply_markdown(update, template)
        
    @staticmethod
    async def lp_balance_callback(update: Update, context: CallbackContext) -> None:
        logging.info(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] \\balance triggered")
        
        args = context.args
        if len(args) != 1:
            await reply_message(update,
                                f"Please add your wallet as an argument!")
            return
            
        wallet = args[0].lower().strip()\
            .replace("<", "").replace(">", "")\
            .replace("[", "").replace("]", "")
            
        logging.info(f"")
            
        lp_balances = get_lp_balances(wallet)
        msg = format_dict(wallet, lp_balances)
        
        await reply_markdown(update, msg)

    #### bot functions ####

    def add_command_handler(
        self,
        command: str,
        callback: callable
    ) -> None:
        self.app.add_handler(
            CommandHandler(
                command=command,
                callback=callback
            )
        )

    def add_default_handler(
        self,
    ) -> None:
        self.app.add_handler(
            MessageHandler(
                filters=filters.TEXT & ~filters.COMMAND,
                callback=LTFLPBalanceBot.start_callback
            )
        )

    def run(self) -> None:
        logging.info("Bot started!")
        self.app.run_polling()
    