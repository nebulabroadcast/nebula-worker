__all__ = ["Asset", "Item", "Bin", "Event", "User", "anonymous"]

from nebula.objects.asset import Asset
from nebula.objects.bin import Bin
from nebula.objects.event import Event
from nebula.objects.item import Item
from nebula.objects.user import User

anonymous_data = {"login": "Anonymous"}
anonymous = User(meta=anonymous_data)
