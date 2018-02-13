from time import sleep, time
import os
from os import path
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import timedelta, datetime
from typing import Dict, NamedTuple, Tuple, List

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
    msg['To'] = ", ".join(to)  # in case of multiple recipients
    msg['Subject'] = f"Cryptrage report {now}"
    body = "\n".join([pprint(record=spread) for spread in spreads.values()])
    logger.info(f"Message body is {body}")
    msg.attach(MIMEText(body, 'plain'))
    text = msg.as_string()
    server.sendmail(user, to, text)


def _check_spreads(*, spreads: List[NamedTuple], spreads_to_email: Dict[str, NamedTuple],
                   emailed_spreads: Dict[str, NamedTuple],
                   open_for: timedelta=timedelta(seconds=10),
                   dont_email_newer_than: timedelta=timedelta(hours=2),
                   **kwargs) -> Tuple[Dict[str, NamedTuple], Dict[str, NamedTuple]]:
    current_spreads = {getattr(spread, 'exchanges_hash'): spread for spread in spreads}
    logger.info(f"Got spreads, namely {current_spreads}")
    # the order ensures the **older** timestamp in spreads_to_email will overwrite the newer!!
    spreads_to_email = {**current_spreads, **spreads_to_email}
    older_than = get_between_deltas(td_min=open_for,
                                    td_max=dont_email_newer_than,
                                    spreads=spreads_to_email,
                                    time_attr='sell_to_ts')
    emailed_spreads = get_between_deltas(td_min=timedelta(0),
                                         td_max=dont_email_newer_than,
                                         spreads=emailed_spreads,
                                         time_attr='sell_to_ts')
    # TODO first check that this does what it should, then move it
    logger.info(f"Spreads open for more than {open_for.total_seconds()} seconds "
                f"are {older_than}")
    logger.info(f"Spreads emailed less than {dont_email_newer_than.total_seconds()} "
                f"seconds ago are {emailed_spreads}")
    # spreads that were emailed less than `dont_email_newer_than`, should not be emailed again
    ready_to_send = {el: older_than[el] for el in older_than if el not in emailed_spreads}
    if ready_to_send:
        logger.info(f"Spreads to email are {ready_to_send}")
        send(spreads=ready_to_send, **kwargs)
        emailed_spreads = {**emailed_spreads, **ready_to_send}
        spreads_to_email = {}
    return spreads_to_email, emailed_spreads


def check_spreads(*, pool: AbstractConnectionPool, transaction_pct: float=0.25, sleep_for: int=5,
                  open_for: timedelta=timedelta(seconds=10),
                  dont_email_newer_than: timedelta=timedelta(hours=2), **kwargs) -> None:
    """
    Send an email if a spread if open for more than open_for seconds
    """
    # TODO Good check on everything needs to take place
    spreads_to_email = {}
    emailed_spreads = {}
    while True:
        spreads = get_spreads(pool=pool, table='ticker', transaction_pct=transaction_pct)
        if spreads:
            spreads_to_email, emailed_spreads = _check_spreads(spreads=spreads,
                                                               spreads_to_email=spreads_to_email,
                                                               emailed_spreads=emailed_spreads,
                                                               open_for=open_for,
                                                               dont_email_newer_than=dont_email_newer_than,
                                                               **kwargs)
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
                    "to": ["giovanni@lanzani.nl", "rogier.de.jonge@gmail.com"]}
    dsn = f"host=localhost password={pgpassword} dbname=timescale user=timescale"
    logger.info("Initializing connection pool")
    pool = ThreadedConnectionPool(minconn=1, maxconn=2, dsn=dsn)
    logger.info("Starting spread checker")
    check_spreads(pool=pool, transaction_pct=0.25, sleep_for=5, open_for=timedelta(seconds=10),
                  dont_email_newer_than=timedelta(hours=2), **email_kwargs)


if __name__ == "__main__":
    main()
