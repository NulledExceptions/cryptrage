from time import sleep, time
import os
from os import path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import timedelta, datetime
from typing import Dict, NamedTuple

from psycopg2.pool import ThreadedConnectionPool, AbstractConnectionPool
from pid.decorator import pidfile

from cryptrage.database.get_spreads import get_spreads
from cryptrage.external_api import localize_timestamp
from cryptrage.logging import setup_logging

config_path = path.join(path.dirname(path.abspath(__file__)), 'configure', 'send_email.yaml')
logger = setup_logging(config_path=config_path, name='send')


def get_between_deltas(*, td_min: timedelta=timedelta(seconds=10),
                       td_max: timedelta=timedelta(seconds=20),
                       spreads: Dict[str, NamedTuple],
                       time_attr: str) -> Dict[str, NamedTuple]:
    now = localize_timestamp(time())
    results = {}
    for key, value in spreads.items():
        if ((now - getattr(value, time_attr) > td_min) and
            (now - getattr(value, time_attr) < td_max)):
            results[key] = value
    return results


def send_email(*, older_than: Dict[str, NamedTuple], emailed_spreads: Dict[str, NamedTuple],
               td: timedelta=timedelta(hours=2), time_attr: str, **kwargs) -> Dict[str, NamedTuple]:
    now = localize_timestamp(time())
    to_send = {}
    for key, value in older_than.items():
        if (key not in emailed_spreads) or (now - getattr(value, time_attr) > td):  # email already sent
            to_send[key] = value
    logger.info(f"Spreads to email are {to_send}")
    send(spreads=to_send, **kwargs)
    emailed_spreads = {**emailed_spreads, **to_send}
    logger.info(f"Emailed spreads in the last {td} are {emailed_spreads}")

    return emailed_spreads


def pprint(*, record: NamedTuple) -> str:
    return "\n" + "\n".join([f"{key}: {value}" for key, value in record._asdict().items()])


def send(*, spreads: Dict[str, NamedTuple], server_addr: str, user: str, password: str,
         port: int, to: str):
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


def check_spread(*, pool: AbstractConnectionPool, transaction_pct: float=0.25, sleep_for: int=5,
                 open_for: int=10, dont_email_newer_than: int=2, **kwargs) -> None:
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
            older_than = get_between_deltas(td_min=timedelta(seconds=open_for),
                                            td_max=timedelta(seconds=open_for * 2),
                                            spreads=spreads_to_email,
                                            time_attr='sell_to_ts')
            if older_than:
                logger.info(f"Spreads open for more than {open_for} are {older_than}")

                emailed_spreads = send_email(older_than=older_than,
                                             emailed_spreads=emailed_spreads,
                                             time_attr='sell_to_ts',
                                             td=timedelta(hours=dont_email_newer_than),
                                             **kwargs)
                spreads_to_email = {}
        sleep(sleep_for)


@pidfile(pidname='emailer', piddir='.')
def main() -> None:

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
                 dont_email_newer_than=2, **email_kwargs)


if __name__ == "__main__":
    main()