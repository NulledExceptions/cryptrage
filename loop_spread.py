from time import sleep, time
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import timedelta, datetime
from typing import Dict, NamedTuple, Tuple

from psycopg2.pool import ThreadedConnectionPool, AbstractConnectionPool
from pid.decorator import pidfile

from cryptrage.database.get_spreads import get_spreads
from cryptrage.external_api import localize_timestamp


def get_older_than(*, td: timedelta=timedelta(minutes=10), spreads: Dict[str, NamedTuple],
                   time_attr: str) -> Dict[str, NamedTuple]:
    now = localize_timestamp(time())
    results = {}
    for key, value in spreads.items():
        if now - getattr(value, time_attr) > td:
            results[key] = value
    return results


def send_email(*, older_than: Dict[str, NamedTuple], emailed_spreads: Dict[str, NamedTuple],
               td: timedelta=timedelta(hours=2),
               time_attr: str, **kwargs) -> Tuple[Dict[str, NamedTuple], Dict[str, NamedTuple]]:
    if older_than:
        now = localize_timestamp(time())
        to_send = {}
        for key, value in older_than:
            if (key not in emailed_spreads) or (now - getattr(value, time_attr) > td):  # email already sent
                to_send[key] = value
        emailed_spreads = {**emailed_spreads, **to_send}
        send(spreads=to_send, **kwargs)
    return {}, emailed_spreads


def pprint(*, record: NamedTuple) -> str:
    return "\n" + "\n".join([f"{key}: {value}" for key, value in record._asdict().items()])


def send(*, spreads: Dict[str, NamedTuple], server: str, user: str, password: str,
         port: int, to: str):
    now = datetime.now()
    server = smtplib.SMTP(server, port)
    server.ehlo()
    server.starttls()
    server.ehlo()
    server.login(user, password)
    msg = MIMEMultipart()
    msg['From'] = user
    msg['To'] = to
    msg['Subject'] = f"Cryptrage report {now}"
    body = "\n".join([pprint(record=spread) for spread in spreads.values()])
    msg.attach(MIMEText(body, 'plain'))
    text = msg.as_string()
    server.sendmail(user, to, text)


def check_spread(*, pool: AbstractConnectionPool, transaction_pct: float=0.25, sleep_for: int=20, **kwargs) -> None:
    """
    Send an email if a spread if open for more than 10 minutes
    """
    spreads_to_email = {}
    emailed_spreads = {}
    while True:
        spreads = get_spreads(pool=pool, table='ticker', transaction_pct=transaction_pct)
        if spreads:
            current_spreads = {getattr(spread, 'exchanges_hash'): spread for spread in spreads}
            spreads_to_email = {**current_spreads, **spreads_to_email}
            older_than = get_older_than(td=timedelta(minutes=10), spreads=spreads_to_email,
                                        time_attr='sell_to_ts')
            spreads_to_email, emailed_spreads = send_email(older_than=older_than,
                                                           emailed_spreads=emailed_spreads,
                                                           time_attr='sell_to_ts',
                                                           td=timedelta(hours=2), **kwargs)
        sleep(sleep_for)


@pidfile(pidname='emailer', piddir='.')
def main() -> None:
    pgpassword = os.environ.get("PGPASSWORD")

    if not pgpassword:
        raise ValueError("PGPASSWORD was not a valid environment variable")
    email_pw = os.environ.get("EMAILPW")
    email_kwargs = {"server": 'smtp-mail.outlook.com',
                    "user": 'cryptrage@outlook.com',
                    "password": email_pw,
                    "port": 587,
                    "to": "giovanni@lanzani.nl"}
    dsn = f"host=localhost password={pgpassword} dbname=timescale user=timescale"
    pool = ThreadedConnectionPool(minconn=1, maxconn=2, dsn=dsn)
    check_spread(pool=pool, transaction_pct=0.25, sleep_for=20, **email_kwargs)


if __name__ == "__main__":
    main()
