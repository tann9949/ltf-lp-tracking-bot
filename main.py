import logging

from dotenv import load_dotenv

from src.bot import LTFLPBalanceBot

# setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def main() -> None:
    if load_dotenv():
        logging.info(".env loaded!")
    
    bot = LTFLPBalanceBot()
    bot.run()


if __name__ == "__main__":
    main()
