from enum import Enum
from typing import Dict, Union

from google.cloud.firestore_v1.query import Query
from google.api_core.datetime_helpers import DatetimeWithNanoseconds


# https://googleapis.dev/python/firestore/latest/query.html#google.cloud.firestore_v1.query.Query.order_by
class OrderByDirection(Enum):
    Ascending = Query.ASCENDING
    Descending = Query.DESCENDING


FirestoreTimestamp = Union[Dict[str, int], DatetimeWithNanoseconds]
