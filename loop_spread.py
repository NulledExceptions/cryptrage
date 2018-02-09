from time import sleep, time
import os
from os import path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import timedelta, datetime
from typing import Dict, NamedTuple, Tuple
import logging

from psycopg2.pool import ThreadedConnectionPool, AbstractConnectionPool
from pid.decorator import pidfile

from cryptrage.database.get_spreads import get_spreads
from cryptrage.external_api import localize_timestamp

from cryptrage.logging import setup_logging, add_logger


def get_older_than(*, td: timedelta=timedelta(minutes=10), spreads: Dict[str, NamedTuple],
                   time_attr: str) -> Dict[str, NamedTuple]:
    now = localize_timestamp(time())
    results = {}
    for key, value in spreads.items():
        if now - getattr(value, time_attr) > td:
            results[key] = value
    return results


@add_logger
def send_email(*, older_than: Dict[str, NamedTuple], emailed_spreads: Dict[str, NamedTuple],
               td: timedelta=timedelta(hours=2), time_attr: str,
               logger=None, **kwargs) -> Tuple[Dict[str, NamedTuple], Dict[str, NamedTuple]]:
    if older_than:
        now = localize_timestamp(time())
        to_send = {}
        for key, value in older_than.items():
            if (key not in emailed_spreads) or (now - getattr(value, time_attr) > td):  # email already sent
                to_send[key] = value
        logger.info(f"Spreads to email are {to_send}")
        send(spreads=to_send, logger=logger, **kwargs)
        emailed_spreads = {**emailed_spreads, **to_send}
        logger.info(f"Emailed spreads in the last {td} are {emailed_spreads}")

    return {}, emailed_spreads


def pprint(*, record: NamedTuple) -> str:
    return "\n" + "\n".join([f"{key}: {value}" for key, value in record._asdict().items()])


@add_logger
def send(*, spreads: Dict[str, NamedTuple], server_addr: str, user: str, password: str,
         port: int, to: str, logger=None):
    logger.info(f"Received {len(spreads)} spreads to email")
    now = datetime.now()
    server = smtplib.SMTP(server_addr, port)
    logger.info(f"Initialized server {server}")
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(user, password)
    logger.info(f"Login in {server_addr} successful")
    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = to
    msg['Subject'] = f"Cryptrage report {now}"
    body = "\n".join([pprint(record=spread) for spread in spreads.values()])
    logger.info(f"Message body is {body}")
    msg.attach(MIMEText(body, 'plain'))
    text = msg.as_string()
    server.sendmail(user, to, text)


@add_logger
def check_spread(*, pool: AbstractConnectionPool, transaction_pct: float=0.25, sleep_for: int=5,
                 open_for: int=10, dont_email_newer_than: int=2, logger=None, **kwargs) -> None:
    """
    Send an email if a spread if open for more than open_for seconds
    """
    spreads_to_email = {}
    emailed_spreads = {}
    while True:
        spreads = get_spreads(pool=pool, table='ticker', transaction_pct=transaction_pct)
        if spreads:
            current_spreads = {getattr(spread, 'exchanges_hash'): spread for spread in spreads}
            logger.info(f"Got spreads, namely {current_spreads}")
            # the order ensures the **older** timestamp in spreads_to_email will overwrite the newer!!
            spreads_to_email = {**current_spreads, **spreads_to_email}
            older_than = get_older_than(td=timedelta(seconds=open_for), spreads=spreads_to_email,
                                        time_attr='sell_to_ts')
            if older_than:
                logger.info(f"Spreads open for more than {open_for} are {older_than}")

                spreads_to_email, emailed_spreads = send_email(older_than=older_than,
                                                               emailed_spreads=emailed_spreads,
                                                               time_attr='sell_to_ts',
                                                               td=timedelta(hours=dont_email_newer_than),
                                                               **kwargs)
        sleep(sleep_for)


@pidfile(pidname='emailer', piddir='.')
def main() -> None:
    config_path = path.join(path.dirname(path.abspath(__file__)), 'configure', 'send_email.yaml')
    logger = setup_logging(config_path=config_path, name='send')
    pgpassword = os.environ.get("PGPASSWORD")

    if not pgpassword:
        raise ValueError("PGPASSWORD was not a valid environment variable")
    email_pw = os.environ.get("EMAILPW")
    if not email_pw:
        raise ValueError("EMAILPW was not a valid environment variable")
    email_kwargs = {"server_addr": 'smtp.gmail.com',
                    "user": 'pycryptrage@gmail.com',
                    "password": email_pw,
                    "port": 587,
                    "to": "giovanni@lanzani.nl"}
    dsn = f"host=localhost password={pgpassword} dbname=timescale user=timescale"
    logger.info("Initializing connection pool")
    pool = ThreadedConnectionPool(minconn=1, maxconn=2, dsn=dsn)
    logger.info("Starting spread checker")
    check_spread(pool=pool, transaction_pct=0.25, sleep_for=5, open_for=10,
                 dont_email_newer_than=2, **email_kwargs, logger=logger)


if __name__ == "__main__":
    main()