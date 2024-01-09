import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Literal

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


def asset_by_path(id_storage: int, path: str, db: DB | None = None) -> Asset | None:
    id_storage = id_storage
    path = path.replace("\\", "/")
    if db is None:
        db = DB()
    db.query(
        """
            SELECT meta FROM assets
                WHERE media_type = %s
                AND meta->>'id_storage' = %s
                AND meta->>'path' = %s
        """,
        [MediaType.FILE, str(id_storage), path],
    )
    for (meta,) in db.fetchall():
        return Asset(meta=meta, db=db)
    return None


def asset_by_full_path(path: str, db: DB | None = None) -> Asset | None:
    if db is None:
        db = DB()
    for storage in storages:
        if path.startswith(storage.local_path):
            return asset_by_path(
                storage.id,
                path.replace(storage.local_path, "", 1),
                db=db,
            )
    return None


def meta_exists(key: str, value: Any, db: DB | None = None) -> Asset | None:
    if db is None:
        db = DB()
    db.query("SELECT meta FROM assets WHERE meta->>%s = %s", [str(key), str(value)])
    for (meta,) in db.fetchall():
        return Asset(meta=meta, db=db)
    return None


def get_day_events(id_channel: int, date: str, num_days: int = 1):
    chconfig = settings.get_playout_channel(id_channel)
    if chconfig is None:
        return

    start_time = datestr2ts(date, *chconfig.day_start)
    end_time = start_time + (3600 * 24 * num_days)
    db = DB()
    db.query(
        """
        SELECT meta
        FROM events
        WHERE id_channel=%s
        AND start > %s
        AND start < %s
        """,
        (id_channel, start_time, end_time),
    )
    for (meta,) in db.fetchall():
        yield Event(meta=meta)


def get_bin_first_item(id_bin: int, db: DB | None = None) -> Item | None:
    if not db:
        db = DB()
    db.query(
        """
        SELECT meta FROM items
        WHERE id_bin=%s
        ORDER BY position LIMIT 1
        """,
        [id_bin],
    )
    for (meta,) in db.fetchall():
        return Item(meta=meta, db=db)
    return None


def get_item_event(id_item: int, db: DB | None = None) -> Event | None:
    if db is None:
        db = DB()
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
    return None


def get_item_runs(
    id_channel: int,
    from_ts: float,
    to_ts: float,
    db: DB | None = None,
) -> dict[int, tuple[float, float]]:
    if db is None:
        db = DB()
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


def get_next_item(
    item: int | Item,
    db: DB | None = None,
    force: Literal["prev", "next"] | None = None,
    force_next_event: bool = False,
) -> Item | None:
    if db is None:
        db = DB()
    if isinstance(item, int) and item > 0:
        current_item = Item(item, db=db)
    elif isinstance(item, Item):
        current_item = item
    else:
        log.error(f"Unexpected get_next_item argument {item}")
        return None

    if not current_item.id:
        return None

    log.debug(f"Looking for an item following {current_item}")
    current_bin = Bin(current_item["id_bin"], db=db)

    items_in_bin = current_bin.items
    if force == "prev":
        items_in_bin.reverse()

    for item_in_bin in items_in_bin:
        ipos = item_in_bin["position"]
        cpos = current_item["position"]

        # This should never happen, just keep mypy happy
        assert isinstance(ipos, int), f"ipos {ipos} is not an int"
        assert isinstance(cpos, int), f"cpos {cpos} is not an int"

        if (force == "prev" and ipos < cpos) or (force != "prev" and ipos > cpos):
            if item_in_bin["item_role"] == "lead_out" and not force:
                log.info("Cueing Lead In")
                for r in current_bin.items:
                    if r["item_role"] == "lead_in":
                        return r
                else:
                    next_item = current_bin.items[0]
                    _ = next_item.asset  # force asset preload
                    return next_item
            if item_in_bin["run_mode"] == RunMode.RUN_SKIP:
                continue
            _ = item_in_bin.asset  # force asset preload
            return item_in_bin
    else:
        current_event = get_item_event(current_item.id, db=db)
        if not current_event:
            return None  # shouldn't happen, just keep mypy happy

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
            log.debug(f"End of block. Next {next_event}")
            _ = next_event.bin  # force bin preload
            assert next_event.bin, f"{next_event} event has no bin"
            assert next_event.bin.items, f"{next_event.bin} bin has no items"
            assert not (
                next_event["run_mode"] and not force_next_event
            ), f"Next playlist run mode is not auto {next_event}"

            if force == "prev":
                next_item = next_event.bin.items[-1]
            else:
                next_item = next_event.bin.items[0]
            _ = next_item.asset  # force asset preload
            return next_item
        except AssertionError as e:
            log.info(f"Looping {current_event}: {e}")
        except IndexError:
            log.info(f"Looping {current_event}: no next event")
        except Exception:
            log.traceback(f"Error: looping {current_event} as fallback")
        next_item = current_bin.items[0]
        _ = next_item.asset  # force asset preload
        return next_item


def bin_refresh(
    bins: list[int],
    db: DB | None = None,
    initiator: str | None = None,
):
    bins = [b for b in bins if b]
    if not bins:
        return

    if db is None:
        db = DB()

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
    log.debug(f"Bins changed {bins}")
    messaging.send(
        "objects_changed",
        objects=bins,
        object_type="bin",
        initiator=initiator,
    )
    if changed_events:
        log.debug(f"Events changed {bins}")
        messaging.send(
            "objects_changed",
            objects=changed_events,
            object_type="event",
            initiator=initiator,
        )


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
        assert mistune, "Mistune not installed"
        msg = MIMEMultipart("alternative")
        html = mistune.html(text)
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")
        msg.attach(part1)
        msg.attach(part2)
        return msg
    else:
        return MIMEText(text, "plain")


def send_mail(to: str | list[str], subject: str, body: str, **kwargs):
    try:
        assert settings.system.smtp_host, "SMTP host not configured"
        assert settings.system.smtp_port, "SMTP port not configured"
        assert settings.system.smtp_user, "SMTP user not configured"
        assert settings.system.smtp_password, "SMTP password not configured"
        assert settings.system.mail_from, "Mail from not configured"
    except AssertionError as e:
        log.error(f"Mail not sent: {e}")
        return False

    if isinstance(to, str):
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
    if settings.system.smtp_user and settings.system.smtp_password:
        s.login(settings.system.smtp_user, settings.system.smtp_password)
    s.sendmail(reply_address, to, msg.as_string())
