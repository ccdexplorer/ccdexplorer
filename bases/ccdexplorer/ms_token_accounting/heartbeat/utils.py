from enum import Enum


class Queue(Enum):
    """
    Type of queue to store messages in to send to MongoDB.
    Names correspond to the collection names.
    """

    logged_events = 8
    token_addresses_to_redo_accounting = 9
    token_accounts = 13
    token_addresses = 14
    token_links = 15
