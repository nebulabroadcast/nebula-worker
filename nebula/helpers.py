import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from nxtools import datestr2ts

from nebula.db import DB
from nebula.enum import MediaType, RunMode
from nebula.log import log
from nebula.messaging import messaging
from nebula.objects import Asset, Bin, Event, Item
from nebula.settings import settings
from nebula.storages import storages

try:
    import mistune  # noqa

    has_mistune = True
except ModuleNotFoundError:
    has_mistune = False


def asset_by_path(id_storage, path, db=False):
    id_storage = str(id_storage)
    path = path.replace("\\", "/")
    if not db:
        db = DB()
    db.query(
        """
            SELECT id, meta FROM assets
                WHERE media_type = %s
                AND meta->>'id_storage' = %s
                AND meta->>'path' = %s
        """,
        [MediaType.FILE, id_storage, path],
    )
    for id, meta in db.fetchall():
        return Asset(meta=meta, db=db)
    return False


def asset_by_full_path(path, db=False):
    if not db:
        db = DB()
    for storage in storages:
        if path.startswith(storage.local_path):
            return asset_by_path(storage.id, path.replace(storage.local_path, 1), db=db)
    return False


def meta_exists(key, value, db=False):
    if not db:
        db = DB()
    db.query("SELECT id, meta FROM assets WHERE meta->>%s = %s", [str(key), str(value)])
    for _, meta in db.fetchall():
        return Asset(meta=meta, db=db)
    return False


def get_day_events(id_channel, date, num_days=1):
    chconfig = settings.get_playout_channel(id_channel)

    start_time = datestr2ts(date, *chconfig.day_start)
    end_time = start_time + (3600 * 24 * num_days)
    db = DB()
    db.query(
        """
        SELECT id, meta
        FROM events
        WHERE id_channel=%s
        AND start > %s
        AND start < %s
        """,
        (id_channel, start_time, end_time),
    )
    for _, meta in db.fetchall():
        yield Event(meta=meta)


def get_bin_first_item(id_bin, db=False):
    if not db:
        db = DB()
    db.query(
        """
        SELECT id, meta FROM items
        WHERE id_bin=%s
        ORDER BY position LIMIT 1
        """,
        [id_bin],
    )
    for _, meta in db.fetchall():
        return Item(meta=meta, db=db)
    return False


def get_item_event(id_item, **kwargs):
    db = kwargs.get("db", DB())
    playout_channel_ids = ", ".join([str(f.id) for f in settings.playout_channels])
    db.query(
        f"""
        SELECT e.id, e.meta
        FROM items AS i, events AS e
        WHERE e.id_magic = i.id_bin
        AND i.id = {id_item}
        AND e.id_channel in ({playout_channel_ids})
        """
    )
    for _, meta in db.fetchall():
        return Event(meta=meta, db=db)
    return False


def get_item_runs(id_channel, from_ts, to_ts, db=False):
    db = db or DB()
    db.query(
        """
        SELECT id_item, start, stop
        FROM asrun
        WHERE start >= %s
        AND start < %s
        ORDER BY start DESC
        """,
        [int(from_ts), int(to_ts)],
    )
    result = {}
    for id_item, start, stop in db.fetchall():
        if id_item not in result:
            result[id_item] = (start, stop)
    return result


def get_next_item(item, **kwargs):
    db = kwargs.get("db", DB())
    force = kwargs.get("force", False)
    if type(item) == int and item > 0:
        current_item = Item(item, db=db)
    elif isinstance(item, Item):
        current_item = item
    else:
        log.error(f"Unexpected get_next_item argument {item}")
        return False

    log.debug(f"Looking for an item following {current_item}")
    current_bin = Bin(current_item["id_bin"], db=db)

    items = current_bin.items
    if force == "prev":
        items.reverse()

    for item in items:
        if (force == "prev" and item["position"] < current_item["position"]) or (
            force != "prev" and item["position"] > current_item["position"]
        ):
            if item["item_role"] == "lead_out" and not force:
                log.info("Cueing Lead In")
                for i, r in enumerate(current_bin.items):
                    if r["item_role"] == "lead_in":
                        return r
                else:
                    next_item = current_bin.items[0]
                    next_item.asset
                    return next_item
            if item["run_mode"] == RunMode.RUN_SKIP:
                continue
            item.asset
            return item
    else:
        current_event = get_item_event(item.id, db=db)
        direction = ">"
        order = "ASC"
        if force == "prev":
            direction = "<"
            order = "DESC"
        db.query(
            f"""
            SELECT meta FROM events
            WHERE id_channel = %s and start {direction} %s
            ORDER BY start {order} LIMIT 1
            """,
            [current_event["id_channel"], current_event["start"]],
        )
        try:
            next_event = Event(meta=db.fetchall()[0][0], db=db)
            if not next_event.bin.items:
                log.debug("Next playlist is empty")
                raise Exception
            if next_event["run_mode"] and not kwargs.get("force_next_event", False):
                log.debug("Next playlist run mode is not auto")
                raise Exception
            if force == "prev":
                next_item = next_event.bin.items[-1]
            else:
                next_item = next_event.bin.items[0]
            next_item.asset
            return next_item
        except Exception:
            log.info("Looping current playlist")
            next_item = current_bin.items[0]
            next_item.asset
            return next_item


def bin_refresh(bins, **kwargs):
    bins = [b for b in bins if b]
    if not bins:
        return True
    db = kwargs.get("db", DB())
    sender = kwargs.get("sender", False)
    for id_bin in bins:
        b = Bin(id_bin, db=db)
        b.save(notify=False)
    bq = ", ".join([str(b) for b in bins if b])
    changed_events = []
    db.query(
        f"""
        SELECT e.meta FROM events as e, channels AS c
        WHERE
            c.channel_type = 0 AND
            c.id = e.id_channel AND
            e.id_magic IN ({bq})
        """
    )
    for (meta,) in db.fetchall():
        event = Event(meta=meta, db=db)
        if event.id not in changed_events:
            changed_events.append(event.id)
    log.debug(f"Bins changed {bins}.", f"Initiator {kwargs.get('initiator', log.user)}")
    messaging.send(
        "objects_changed",
        sender=sender,
        objects=bins,
        object_type="bin",
        initiator=kwargs.get("initiator", None),
    )
    if changed_events:
        log.debug(
            f"Events changed {bins}.Initiator {kwargs.get('initiator', log.user)}"
        )
        messaging.send(
            "objects_changed",
            sender=sender,
            objects=changed_events,
            object_type="event",
            initiator=kwargs.get("initiator", None),
        )
    return True


def html2email(html):
    msg = MIMEMultipart("alternative")
    text = "no plaitext version available"
    part1 = MIMEText(text, "plain")
    part2 = MIMEText(html, "html")

    msg.attach(part1)
    msg.attach(part2)

    return msg


def markdown2email(text):
    if has_mistune:
        msg = MIMEMultipart("alternative")
        html = mistune.html(text)
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")
        msg.attach(part1)
        msg.attach(part2)
        return msg
    else:
        return MIMEText(text, "plain")


def send_mail(to, subject, body, **kwargs):
    if type(to) == str:
        to = [to]
    reply_address = kwargs.get("from", settings.system.mail_from)

    if isinstance(body, MIMEMultipart):
        msg = body
    else:
        msg = MIMEText(body)

    msg["Subject"] = subject
    msg["From"] = reply_address
    msg["To"] = ",".join(to)
    if settings.system.smtp_port == 25:
        s = smtplib.SMTP(settings.system.smtp_host, port=25)
    else:
        s = smtplib.SMTP_SSL(settings.system.smtp_host, port=settings.system.smtp_port)
    if settings.system.smtp_user and settings.system.smtp_pass:
        s.login(settings.system.smtp_user, settings.system.smtp_pass)
    s.sendmail(reply_address, [to], msg.as_string())
