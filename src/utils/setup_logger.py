from colorama import Fore, Style, init
from loguru import logger

# Initialize colorama
init(autoreset=True)

# Add a sink for the logger to output to a file
logger.add("./logs/log_for_testing.log")


def log_info(message: str):
    """
    Logs an info message to the console.

    use colorama to color the message blue
    Args:
        message (str): str message to log
    """
    logger.info(f"{Fore.BLUE}{message}{Style.RESET_ALL}")


def log_error(message: str):
    """
    Logs an error message to the console. use colorama to color the
    message red.

    Args:
        message (str): str message to log
    """
    logger.error(f"{Fore.MAGENTA}{message}{Style.RESET_ALL}")


def log_success(message: str):
    """
    Logs a success message to the console. use colorama to color the
    message green.

    Args:
        message (str): _description_
    """
    logger.success(f"{Fore.CYAN}{message}{Style.RESET_ALL}")


# test the logger
if __name__ == "__main__":
    log_info("This is an info message.")
    log_error("This is an error message.")
    log_success("This is a success message.")
