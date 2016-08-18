
import datetime
import ujson
import logging

from tornado.gen import coroutine, Return

from common.database import DuplicateError, DatabaseError
from common.profile import ProfileError
from common.internal import Internal, InternalError
from common.model import Model

import common.database
import common.keyvalue
import common.profile


class CategoryNotFound(Exception):
    pass


class EventAdapter(object):

    STATUS_NOT_STARTED = "not_started"
    STATUS_COMPLETED = "completed"
    STATUS_ACTIVE = "active"

    def __init__(self, record):
        self.item_id = record.get("id")
        self.category_id = record.get("category_id", 0)
        self.category = record.get("category_name", "")
        self.data = record.get("data_json") or {}
        self.custom = record.get("custom")
        self.time_start = record["start_dt"]
        self.time_end = record["end_dt"]
        self.score = record.get("score") or 0
        self.enabled = record.get("enabled", "false") == "true"
        self.tournament = record.get("tournament", "false") == "true"
        self.joined = record.get("status", "NONE") == "JOINED"

    def dump(self):
        e = {
            "id": self.item_id,
            "enabled": self.enabled,
            "category": self.category,
            "time": {
                "start": str(self.time_start),
                "end": str(self.time_end),
                "left": self.time_left()
            },
            "status": self.status(),
            "joined": self.joined,
            "score": self.score
        }

        e.update(self.data)

        if self.tournament:
            e.update({
                "tournament":
                    {
                        "leaderboard_name": EventAdapter.tournament_leaderboard_name(self.item_id),
                        "leaderboard_order": EventAdapter.tournament_leaderboard_order()
                    }
            })

        if self.custom:
            e["profile"] = self.custom

        return e

    def status(self):
        dt_now = datetime.datetime.utcnow()

        if dt_now < self.time_start:
            return EventAdapter.STATUS_NOT_STARTED

        if dt_now >= self.time_end:
            return EventAdapter.STATUS_COMPLETED

        return EventAdapter.STATUS_ACTIVE

    def time_left(self):
        return int((self.time_end - datetime.datetime.utcnow()).total_seconds())

    @staticmethod
    def tournament_leaderboard_name(event_id):
        return "event_" + str(event_id)

    @staticmethod
    def tournament_leaderboard_order():
        return "desc"


class CategoryAdapter(object):
    def __init__(self, data):
        self.category_id = data.get("id")
        self.name = data.get("category_name")
        self.scheme = data.get("scheme_json")


class EventError(Exception):
    def __init__(self, reason, code=400):
        super(EventError, self).__init__(reason)

        self.code = code


class EventNotFound(Exception):
    def __init__(self, event_id):
        self.event_id = event_id


class EventsModel(Model):
    DT_FMT = "%Y-%m-%d %H:%M:%S"

    @coroutine
    def __delete_leaderboard__(self, event_id, gamespace):
        leaderboard_name = EventAdapter.tournament_leaderboard_name(event_id)
        leaderboard_order = EventAdapter.tournament_leaderboard_order()

        try:
            yield self.internal.request(
                "leaderboard", "delete",
                gamespace=gamespace, sort_order=leaderboard_order,
                leaderboard_name=leaderboard_name)

        except InternalError as e:
            logging.exception("Failed to delete a leaderboard: " + e.message)

    def __init__(self, db):
        self.db = db
        self.internal = Internal()

    def get_setup_tables(self):
        return ["common_scheme", "category_scheme", "events", "event_participants"]

    def get_setup_db(self):
        return self.db

    @coroutine
    def __post_score_to_leaderboard__(self, account, gamespace, score, event_id, display_name, expire_in, profile):
        leaderboard_name = EventAdapter.tournament_leaderboard_name(event_id)
        leaderboard_order = EventAdapter.tournament_leaderboard_order()

        try:
            yield self.internal.request(
                "leaderboard", "post",
                account=account, gamespace=gamespace, sort_order=leaderboard_order,
                leaderboard_name=leaderboard_name, score=score, display_name=display_name,
                expire_in=expire_in, profile=profile)
        except InternalError as e:
            logging.exception("Failed to post to leaderboard: " + e.message)

    @coroutine
    def add_score(self, gamespace_id, event_id, account_id, score, leaderboard_info):

        if not isinstance(score, float):
            raise EventError("Score is not a float")

        new_score = 0
        tournament = False

        with (yield self.db.acquire(auto_commit=False)) as db:

            # lookup for existing participation along with some event information
            # current record is locked to avoid concurrency issues
            res = yield db.get(
                """
                    SELECT `score`, (
                            SELECT CONCAT('active', '|', `tournament`)
                            FROM `events` AS e
                            WHERE e.`id` = p.`event_id` AND
                                NOW() BETWEEN e.`start_dt` AND e.`end_dt`
                        ) AS `event_status` FROM `event_participants` AS p
                    WHERE `event_id` = %s AND `account_id` = %s AND `gamespace_id` = %s AND `status` = %s
                    FOR UPDATE;
                """,
                event_id, account_id, gamespace_id, "JOINED")

            if res:
                # if there's a participation, check if the evens is active
                event_status = res["event_status"]
                active = False

                if event_status:
                    active, tournament = event_status.split("|")

                if active != "active":
                    yield db.commit()
                    raise EventError("Event is not active", code=409)

                # get the existent score
                old_score = res["score"]
            else:
                # if user has not been participated in this event, join him
                yield db.commit()
                yield self.join_event(gamespace_id, event_id, account_id, score=score,
                                      leaderboard_info=leaderboard_info)
                raise Return(score)

            # add the score from db to the posted score
            new_score = old_score + score

            # update the score, releasing the lock
            yield db.execute(
                """
                UPDATE `event_participants`
                SET `score` = %s
                WHERE `event_id` = %s AND `account_id` = %s AND `gamespace_id` = %s;
                """, new_score, event_id, account_id, gamespace_id)

            yield db.commit()

            # if there's a tournament attached to this event, post the score to the leaderboard
            if new_score and tournament:
                if leaderboard_info is None:
                    raise EventError("leaderboard_info is required", 400)

                display_name = leaderboard_info.get("display_name")
                expire_in = leaderboard_info.get("expire_in")
                profile = leaderboard_info.get("profile", {})

                if not display_name or not expire_in:
                    raise EventError("Cannot post score to tournament: "
                                     "leaderboard_info should have 'display_name' and 'expire_in' fields", 400)

                yield self.__post_score_to_leaderboard__(
                    account_id, gamespace_id, new_score, event_id,
                    display_name, expire_in, profile)

        raise Return(new_score)

    @coroutine
    def clone_category_scheme(self, gamespace_id, category_id):
        logging.debug("Cloning event category '%s'", category_id)
        category = yield self.get_category(gamespace_id, category_id)

        category_scheme = category.scheme

        if 'title' in category_scheme:
            category_scheme['title'] = 'Clone of ' + category_scheme['title']

        result = yield self.db.insert(
            """
                INSERT INTO `category_scheme` (`gamespace_id`, `scheme_json`)
                SELECT `gamespace_id`, %s
                    FROM `category_scheme`
                    WHERE `id`=%s AND `gamespace_id`=%s
            """,
            ujson.dumps(category_scheme), category_id, gamespace_id)

        raise Return(result)

    @coroutine
    def create_category(self, gamespace_id, category_name, scheme):
        result = yield self.db.insert(
            """
                INSERT INTO `category_scheme`
                (`gamespace_id`, `category_name`, `scheme_json`)
                VALUES
                (%s, %s, %s)
            """,
            gamespace_id, category_name, ujson.dumps(scheme))

        raise Return(result)

    @coroutine
    def create_event(self, gamespace_id, category_id, enabled, tournament, data_json, start_dt, end_dt):

        category = yield self.get_category(gamespace_id, category_id)

        event_id = yield self.db.insert(
            """
                INSERT INTO events
                (gamespace_id, category_id, enabled, tournament, category_name, data_json, start_dt, end_dt)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            gamespace_id, category_id, enabled, tournament, category.name, ujson.dumps(data_json),
            start_dt, end_dt)

        raise Return(event_id)

    @coroutine
    def delete_category(self, gamespace_id, category_id):

        yield self.db.execute(
            """
                DELETE FROM `events`
                WHERE `category_id`=%s AND `gamespace_id`=%s
            """,
            category_id, gamespace_id)

        yield self.db.execute(
            """
                DELETE FROM `category_scheme`
                WHERE `id`=%s AND `gamespace_id`=%s
            """,
            category_id, gamespace_id)

    @coroutine
    def delete_event(self, gamespace_id, event_id):
        # find the event, EventNotFound otherwise
        event = yield self.get_event(gamespace_id, event_id)

        yield self.db.execute(
            """
                DELETE FROM `event_participants`
                WHERE `event_id`=%s AND `gamespace_id`=%s
            """,
            event_id, gamespace_id)

        yield self.db.execute(
            """
                DELETE FROM `events`
                WHERE `id`=%s AND `gamespace_id`=%s
            """,
            event_id, gamespace_id)

        if event.tournament:
            yield self.__delete_leaderboard__(event_id, gamespace_id)

    @coroutine
    def get_category(self, gamespace_id, category_id):
        category = yield self.db.get(
            """
                SELECT *
                FROM category_scheme
                WHERE id = %s AND gamespace_id = %s
            """,
            category_id, gamespace_id)

        if not category:
            raise CategoryNotFound()

        raise Return(CategoryAdapter(category))

    @coroutine
    def get_common_scheme(self, gamespace_id):
        common_scheme = yield self.db.get(
            """
                SELECT scheme_json
                FROM common_scheme
                WHERE gamespace_id = %s
            """,
            gamespace_id)

        if not common_scheme:
            raise Return({})

        common_scheme = common_scheme['scheme_json']
        raise Return(common_scheme)

    @coroutine
    def get_event(self, gamespace_id, event_id):

        event = yield self.db.get(
            """
                SELECT *
                FROM `events`
                WHERE id = %s AND gamespace_id = %s
            """,
            event_id, gamespace_id)

        if event:
            raise Return(EventAdapter(event))

        raise EventNotFound(event_id)

    @coroutine
    def list_categories(self, gamespace_id):
        categories = yield self.db.query(
            """
                SELECT *
                FROM category_scheme
                WHERE gamespace_id = %s
            """,
            gamespace_id)

        raise Return(map(CategoryAdapter, categories))

    @coroutine
    def get_events(self, gamespace_id, account_id):
        events = yield self.db.query(
            """
                SELECT `events`.*,
                    `participant`.`account_id`, `participant`.`status`, `participant`.`score`,
                    `participant`.`custom`
                FROM category_scheme AS evcat
                JOIN `events` ON evcat.id = `events`.category_id
                LEFT JOIN (
                   SELECT * FROM event_participants WHERE account_id = %s
                ) AS `participant` ON (`events`.id = `participant`.event_id)
                WHERE
                    evcat.gamespace_id = %s AND
                    NOW() BETWEEN `events`.start_dt AND `events`.end_dt
            """,
            account_id, gamespace_id)

        raise Return([EventAdapter(event) for event in events])

    @coroutine
    def join_event(self, gamespace_id, event_id, account_id, score=0.0, leaderboard_info=None):

        with (yield self.db.acquire()) as db_conn:
            event_data = yield db_conn.get(
                """
                    SELECT *
                    FROM `events`
                    WHERE `id` = %s AND `gamespace_id` = %s
                """,
                event_id, gamespace_id)

            if event_data:
                event = EventAdapter(event_data)
                status = event.status()

                if status == EventAdapter.STATUS_NOT_STARTED:
                    raise EventError("Event has not started yet")
                elif status == EventAdapter.STATUS_COMPLETED:
                    raise EventError("Event has been ended")

                try:
                    result = yield db_conn.insert(
                        """
                            INSERT INTO `event_participants`
                            (`account_id`, `gamespace_id`, `event_id`, `status`, `score`, `custom`)
                            VALUES (%s, %s, %s, %s, %s, %s);
                        """,
                        account_id, gamespace_id, event_id, "JOINED", score, "{}")

                    if event.tournament:
                        if leaderboard_info is None:
                            raise EventError("leaderboard_info is required")

                        display_name = leaderboard_info.get("display_name")
                        expire_in = leaderboard_info.get("expire_in")
                        profile = leaderboard_info.get("profile", {})

                        if not display_name or not expire_in:
                            raise EventError("Cannot post score to tournament: "
                                             "leaderboard_info should have 'display_name' and 'expire_in' fields", 400)

                        yield self.__post_score_to_leaderboard__(
                            account_id, gamespace_id, score, event_id,
                            display_name, expire_in, profile)

                    raise Return(result)
                except DuplicateError:
                    raise EventError("The user already took part in the event", code=409)
            else:
                raise EventNotFound(event_id)

    @coroutine
    def leave_event(self, gamespace_id, event_id, account_id):

        res = yield self.db.execute(
            """
                UPDATE event_participants
                SET `status`= %s
                WHERE `event_id` = %s AND `account_id` = %s AND `gamespace_id` = %s
            """,
            "LEFT", event_id, account_id, gamespace_id)

        if not res:
            raise EventError(
                "Either the event doesn't exist or the user doesn't participate in it"
            )

        raise Return(res)

    @coroutine
    def list_paged_events(self, gamespace_id, category_id, items_in_page, page):

        filters = []
        params = []

        if category_id and int(category_id):
            filters.append("AND category_id=%s")
            params.append(category_id)

        with (yield self.db.acquire(auto_commit=False)) as db:
            pages_count = yield db.get("""
                SELECT COUNT(*) as `count`
                FROM `events`
                WHERE gamespace_id=%s {0};
            """.format("".join(filters)), gamespace_id, *params)

            import math
            pages = int(math.ceil(float(pages_count["count"]) / float(items_in_page)))

            limit_a = (page - 1) * items_in_page
            limit_b = page * items_in_page

            params += [limit_a, limit_b]

            events = yield db.query(
                """
                    SELECT *
                    FROM `events`
                    WHERE gamespace_id=%s {0}
                    ORDER BY `start_dt` ASC
                    LIMIT %s, %s;
                """.format("".join(filters)), gamespace_id, *params)

            result = [EventAdapter(event) for event in events], pages
            raise Return(result)

    @coroutine
    def update_category(self, gamespace_id, category_id, new_scheme, category_name):

        yield self.db.execute(
            """
                UPDATE category_scheme
                SET scheme_json=%s, category_name=%s
                WHERE id=%s AND gamespace_id=%s
            """,
            ujson.dumps(new_scheme), category_name, category_id, gamespace_id)

        yield self.db.execute(
            """
                UPDATE events
                SET category_name=%s
                WHERE category_id=%s AND gamespace_id=%s
            """,
            category_name, category_id, gamespace_id)

    @coroutine
    def update_common_scheme(self, gamespace_id, new_scheme):

        scheme_id = yield self.db.get(
            """
                SELECT id
                FROM common_scheme
                WHERE gamespace_id=%s
            """,
            gamespace_id)

        if scheme_id and "id" in scheme_id:
            result = yield self.db.execute(
                """
                    UPDATE common_scheme
                    SET scheme_json=%s
                    WHERE id=%s
                """,
                ujson.dumps(new_scheme), int(scheme_id["id"]))
        else:
            result = yield self.db.insert(
                """
                    INSERT INTO common_scheme
                    (gamespace_id, scheme_json) VALUES (%s, %s)
                """,
                gamespace_id, ujson.dumps(new_scheme))

        raise Return(result)

    @coroutine
    def update_event(self, gamespace_id, event_id, enabled, tournament, new_data_json, start_dt, end_dt):

        result = yield self.db.execute(
            """
                UPDATE `events`
                SET `data_json`=%s, `start_dt`=%s, `end_dt`=%s, `enabled`=%s, `tournament`=%s
                WHERE `id`=%s AND `gamespace_id`=%s
            """,
            ujson.dumps(new_data_json), start_dt, end_dt, enabled, tournament, event_id, gamespace_id)

        raise Return(result)

    @coroutine
    def update_profile(self, gamespace_id, event_id, account_id, data, merge=True):

        profile = ParticipationProfile(self.db, gamespace_id, event_id, account_id)

        try:
            result = yield profile.set_data(data, None, merge=merge)
        except common.profile.NoDataError:
            raise EventError("User is not participating in the event")
        except common.profile.ProfileError as e:
            raise EventError("Failed to update event profile: " + e.message)

        raise Return(result)


class ParticipationProfile(common.profile.DatabaseProfile):
    def __init__(self, db, gamespace_id, event_id, account_id):
        super(ParticipationProfile, self).__init__(db)
        self.gamespace_id = gamespace_id
        self.event_id = event_id
        self.account_id = account_id

    @coroutine
    def get(self):
        result = yield self.conn.get(
            """
            SELECT `custom` FROM `event_participants`
            WHERE `event_id` = %s AND `account_id` = %s AND `gamespace_id` = %s
            FOR UPDATE;
            """, self.event_id, self.account_id, self.gamespace_id
        )

        if result:
            raise Return(result["custom"])

        raise common.profile.NoDataError()

    @coroutine
    def insert(self, data):
        raise ProfileError("Insert is not supported")

    @coroutine
    def update(self, data):
        yield self.conn.execute(
            """
            UPDATE event_participants
            SET `custom`= %s
            WHERE `event_id` = %s AND `account_id` = %s AND `gamespace_id` = %s
            """, ujson.dumps(data), self.event_id, self.account_id, self.gamespace_id
        )
