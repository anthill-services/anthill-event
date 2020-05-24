from anthill.common.access import utc_time
from anthill.common.database import DuplicateError, DatabaseError
from anthill.common.profile import ProfileError, NoDataError
from anthill.common.internal import Internal, InternalError
from anthill.common.model import Model
from anthill.common.schedule import Schedule
from anthill.common.options import options
from anthill.common.validate import validate
from anthill.common import Flags, Enum, cached, profile

import datetime
import ujson
import logging
import pytz


class CategoryNotFound(Exception):
    pass


class EventFlags(Flags):
    CLUSTERED = 'clustered'
    TOURNAMENT = 'tournament'
    GROUP = 'group'


class EventEndAction(Enum):
    NONE = 'none'
    MESSAGE = 'message'
    EXEC = 'exec'

    ALL = {
        NONE, MESSAGE, EXEC
    }


EVENT_STATUS_NOT_STARTED = "not_started"
EVENT_STATUS_ENDED = "ended"
EVENT_STATUS_ACTIVE = "active"


class EventParticipationAdapter(object):
    def __init__(self, data):
        self.account_id = data.get("account_id")
        self.status = data.get("participation_status", "NONE")
        self.profile = data.get("participation_profile")
        self.score = data.get("participation_score") or 0
        self.tournament_result = data.get("participation_tournament_result", None)

    joined = property(lambda self: self.status == "JOINED")


class GroupParticipationAdapter(object):
    def __init__(self, data):
        self.group_id = data.get("group_id")
        self.group_status = data.get("group_participation_status", "NONE")
        self.group_profile = data.get("group_participation_profile")
        self.group_score = data.get("group_participation_score") or 0

    group_joined = property(lambda self: self.group_status == "JOINED")


class EventAdapter(object):
    def __init__(self, data):
        self.item_id = data.get("event_id")
        self.category_id = data.get("category_id", 0)
        self.category = data.get("category_name", "")
        self.data = data.get("event_payload") or {}
        self.status = data.get("event_status")
        self.time_start = data["event_start_dt"]
        self.time_end = data["event_end_dt"]
        self.enabled = data.get("event_enabled", 0) == 1
        self.flags = EventFlags(data.get("event_flags", "").lower().split(","))
        self.end_action = EventEndAction(data.get("event_end_action", EventEndAction.NONE))

    tournament = property(lambda self: EventFlags.TOURNAMENT in self.flags)
    clustered = property(lambda self: EventFlags.CLUSTERED in self.flags)
    group = property(lambda self: EventFlags.GROUP in self.flags)

    def kind(self):
        if EventFlags.GROUP in self.flags:
            return "group"
        return "account"

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
            "kind": self.kind()
        }

        e.update(self.data)

        if self.tournament:
            e.update({
                "tournament":
                    {
                        "leaderboard_name": EventAdapter.tournament_leaderboard_name(self.item_id, self.clustered),
                        "leaderboard_order": EventAdapter.tournament_leaderboard_order()
                    }
            })

        return e

    def is_active(self):
        return self.status == EVENT_STATUS_ACTIVE

    def time_left(self):
        return int((self.time_end - datetime.datetime.utcnow()).total_seconds())

    @staticmethod
    def tournament_leaderboard_name(event_id, clustered):
        return ("@" if clustered else "") + "event_" + str(event_id)

    @staticmethod
    def tournament_leaderboard_order():
        return "desc"


class EventWithParticipationAdapter(EventAdapter, EventParticipationAdapter, GroupParticipationAdapter):
    def __init__(self, data):
        EventAdapter.__init__(self, data)
        EventParticipationAdapter.__init__(self, data)
        GroupParticipationAdapter.__init__(self, data)

    def dump(self):
        data = EventAdapter.dump(self)

        if self.tournament and (self.tournament_result is not None):
            tournament = data.get("tournament", None)
            if tournament:
                tournament["result"] = self.tournament_result

        if self.group:
            data.update({
                "score": self.group_score,
                "joined": self.group_joined
            })

            if self.group_profile:
                data["group_profile"] = self.group_profile
        else:
            data.update({
                "score": self.score,
                "joined": self.joined
            })

        if self.profile:
            data["profile"] = self.profile

        return data


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
    pass


class EventSchedule(Schedule):
    def __init__(self, events, check_period):
        super(EventSchedule, self).__init__(check_period)
        self.events = events
        self.db = events.db
        self.check_period = check_period
        self.end_event_actions = {
            EventEndAction.MESSAGE: self.__end_action_message__,
            EventEndAction.EXEC: self.__end_action_exec__,
        }

    async def __end_action_exec__(self, gamespace, event, participants, leaderboard_top_entries=None):
        """
        Once event is finished, calls exec function over batch of participants
        If the event is tournament-like (with leaderboards), each participant rank is also calculated.
        """

        # how many of participants to process in one batched request to exec service
        exec_call_chunk_size = 512

        event_id = event.item_id
        event_data = event.dump()
        group = event.group

        participants_out = []

        if event.tournament:
            if leaderboard_top_entries:
                for cluster_id, cluster in leaderboard_top_entries.items():

                    if not cluster:
                        continue

                    entries = cluster["data"]

                    for entry in entries:

                        account_id = int(entry["account"])
                        participant = participants.get(account_id, None)

                        if not participant:
                            continue

                        if group:
                            participants_out.append({
                                "group": account_id,
                                "score": entry["score"],
                                "rank": entry["rank"],
                                "profile": participant.group_profile
                            })
                        else:
                            participants_out.append({
                                "account": account_id,
                                "score": entry["score"],
                                "rank": entry["rank"],
                                "profile": participant.profile
                            })

        else:
            if group:
                for participant in participants:
                    participants_out.append({
                        "account": participant.group_id,
                        "event": event_data,
                        "score": participant.group_score,
                        "profile": participant.group_profile
                    })
            else:
                for participant in participants:
                    participants_out.append({
                        "group": participant.account_id,
                        "event": event_data,
                        "score": participant.score,
                        "profile": participant.profile
                    })

        if not participants_out:
            return

        def chunks(l, n):
            for i in range(0, len(l), n):
                yield l[i:i + n]

        for chunk in chunks(participants_out, exec_call_chunk_size):

            args = {
                "participants": chunk,
                "event": event_data
            }

            try:
                await self.events.internal.request(
                    "exec", "call_server_function",
                    gamespace=gamespace, method_name="event_completed", args=args, env={})
            except InternalError as e:
                logging.error("Failed to call exec function about completed event "
                              "(" + str(event_id) + "): " + str(e))
            else:
                logging.info("Successfully called exec function about completed event "
                             "(" + str(event_id) + ") to a chunk of participants (" + str(len(chunk)) + ")")

    async def __end_action_message__(self, gamespace, event, participants, leaderboard_top_entries=None):
        """
        Once event is finished, sends a message to every participant.
        If the event is tournament-like (with leaderboards), each participant rank is also calculated.
        """

        event_id = event.item_id

        event_data = event.dump()
        group = event.group

        messages = []

        if event.tournament:
            if leaderboard_top_entries:
                for cluster_id, cluster in leaderboard_top_entries.items():

                    if not cluster:
                        continue

                    entries = cluster["data"]

                    for entry in entries:

                        account_id = int(entry["account"])
                        participant = participants.get(account_id, None)

                        if not participant:
                            continue

                        if group:
                            messages.append({
                                "recipient_class": "social-group",
                                "recipient_key": account_id,
                                "message_type": "event_tournament_result",
                                "payload": {
                                    "event": event_data,
                                    "score": entry["score"],
                                    "rank": entry["rank"],
                                    "profile": participant.group_profile
                                },
                                "flags": []
                            })
                        else:
                            messages.append({
                                "recipient_class": "user",
                                "recipient_key": account_id,
                                "message_type": "event_tournament_result",
                                "payload": {
                                    "event": event_data,
                                    "score": entry["score"],
                                    "rank": entry["rank"],
                                    "profile": participant.profile
                                },
                                "flags": ["remove_delivered"]
                            })

        else:
            if group:
                for participant in participants:
                    messages.append({
                        "recipient_class": "social-group",
                        "recipient_key": participant.group_id,
                        "message_type": "event_tournament_result",
                        "payload": {
                            "event": event_data,
                            "score": participant.group_score,
                            "profile": participant.group_profile
                        },
                        "flags": []
                    })
            else:
                for participant in participants:
                    messages.append({
                        "recipient_class": "user",
                        "recipient_key": participant.account_id,
                        "message_type": "event_tournament_result",
                        "payload": {
                            "event": event_data,
                            "score": participant.score,
                            "profile": participant.profile
                        },
                        "flags": ["remove_delivered"]
                    })

        if not messages:
            return

        def chunks(l, n):
            for i in range(0, len(l), n):
                yield l[i:i + n]

        for chunk in chunks(messages, 1000):

            try:
                await self.events.internal.request(
                    "message", "send_batch",
                    gamespace=gamespace, sender=0, messages=chunk,
                    authoritative=True)
            except InternalError as e:
                logging.error("Failed to deliver reward messages about completed event "
                              "(" + str(event_id) + "): " + str(e))
            else:
                logging.info("Successfully sent reward messages about completed event "
                             "(" + str(event_id) + ") to one chunk of data (" + str(len(chunk)) + ")")

    async def __end_event__(self, gamespace, event):

        event_id = event.item_id
        logging.info("Event {0} has been ended!".format(event_id))

        end_action = self.end_event_actions.get(str(event.end_action), None)

        if event.group:
            participants = await self.events.list_event_group_participants(gamespace, event_id)
        else:
            participants = await self.events.list_event_participants(gamespace, event_id)

        if event.tournament:
            leaderboard_top_entries = await self.events.__get_leaderboard_top__(gamespace, event_id, event.clustered)

            if leaderboard_top_entries:
                ranks = {}

                for cluster_id, cluster in leaderboard_top_entries.items():

                    if not cluster:
                        continue

                    entries = cluster["data"]

                    for entry in entries:
                        account = str(entry["account"])
                        rank = entry["rank"]

                        ranks[account] = rank

                if event.group:
                    await self.events.update_event_group_participants_tournament_result(
                        gamespace, event_id, participants, ranks)
                else:
                    await self.events.update_event_participants_tournament_result(
                        gamespace, event_id, participants, ranks)
        else:
            leaderboard_top_entries = None

        if end_action:
            await end_action(gamespace, event, participants, leaderboard_top_entries)

        await self.db.execute(
            """
                UPDATE `events`
                SET `event_status`=%s, `event_processing`=0
                WHERE `event_id`=%s
                LIMIT 1;
            """, EVENT_STATUS_ENDED, event_id)

    async def __start_event__(self, gamespace, event):

        event_id = event.item_id

        logging.info("Event {0} started!".format(event_id))

        await self.db.execute(
            """
                UPDATE `events`
                SET `event_status`=%s, `event_processing`=0
                WHERE `event_id`=%s
                LIMIT 1;
            """, EVENT_STATUS_ACTIVE, event_id)

    def event_end_cancelled(self, gamespace, event_id, tournament):
        logging.warning("Event {0} cancelled for ending".format(event_id))

        return self.db.execute(
            """
                UPDATE `events`
                SET `event_processing`=0
                WHERE `event_id`=%s
                LIMIT 1;
            """, event_id)

    def event_start_cancelled(self, gamespace, event_id):
        logging.warning("Event {0} cancelled for starting".format(event_id))

        return self.db.execute(
            """
                UPDATE `events`
                SET `event_processing`=0
                WHERE `event_id`=%s
                LIMIT 1;
            """, event_id)

    async def cancelled(self, call_name, *args, **kwargs):

        handlers = {
            "end_event": self.event_end_cancelled,
            "start_event": self.event_start_cancelled
        }

        await handlers[call_name](*args, **kwargs)

    async def update(self):
        async with self.db.acquire(auto_commit=False) as db:
            try:
                events = await db.query(
                    """
                        SELECT *
                        FROM `events`
                        WHERE
                            `event_enabled`=1 AND `event_processing`=0 AND

                            ((`event_status`=%s AND NOW() + INTERVAL %s SECOND > `event_end_dt`)
                            OR
                            (`event_status`=%s AND NOW() + INTERVAL %s SECOND > `event_start_dt`))

                        FOR UPDATE;
                    """, EVENT_STATUS_ACTIVE, self.check_period, EVENT_STATUS_NOT_STARTED, self.check_period)
            except DatabaseError as e:
                raise EventError("Failed to fetch events to update: " + str(e.args[1]), code=500)

            events_ids = [event["event_id"] for event in events]

            if events:
                logging.info("Scheduled {0} events".format(len(events)))

                dt = datetime.datetime.fromtimestamp(utc_time(), tz=pytz.utc)

                for event in events:

                    event_start_dt = event["event_start_dt"].replace(tzinfo=pytz.utc)
                    event_end_dt = event["event_end_dt"].replace(tzinfo=pytz.utc)

                    gamespace = event["gamespace_id"]

                    if event["event_status"] == EVENT_STATUS_ACTIVE:
                        time_left = event_end_dt - dt

                        logging.info("Event {0} will end in {1}.".format(event["event_id"], time_left))

                        self.call(
                            'end_event',
                            self.__end_event__,
                            event_end_dt - dt,
                            gamespace, EventAdapter(event))
                    else:
                        time_passed = event_start_dt - dt

                        logging.info("Event {0} will start in {1}.".format(event["event_id"], time_passed))

                        self.call(
                            'start_event',
                            self.__start_event__,
                            time_passed,
                            gamespace, EventAdapter(event))

                if events_ids:
                    await db.execute(
                        """
                            UPDATE `events`
                            SET `event_processing`=1
                            WHERE `event_id` IN %s;
                        """, events_ids
                    )

            await db.commit()


class EventsModel(Model):
    async def __delete_leaderboard__(self, event_id, gamespace, clustered):
        leaderboard_name = EventAdapter.tournament_leaderboard_name(event_id, clustered)
        leaderboard_order = EventAdapter.tournament_leaderboard_order()

        try:
            await self.internal.request(
                "leaderboard", "delete",
                gamespace=gamespace, sort_order=leaderboard_order,
                leaderboard_name=leaderboard_name)
        except InternalError as e:
            logging.exception("Failed to delete a leaderboard: " + str(e))

    def __init__(self, db, app):
        self.db = db
        self.app = app
        self.internal = Internal()
        self.schedule = EventSchedule(self, options.schedule_update)

    def get_setup_tables(self):
        return ["common_scheme", "category_scheme", "events", "event_participants", "event_group_participants"]

    def get_setup_db(self):
        return self.db

    def has_delete_account_event(self):
        return True

    async def accounts_deleted(self, gamespace, accounts, gamespace_only):
        if gamespace_only:
            await self.db.execute("""
                DELETE 
                FROM `event_participants`
                WHERE `gamespace_id`=%s AND `account_id` IN %s;
            """, gamespace, accounts)
        else:
            await self.db.execute("""
                DELETE 
                FROM `event_participants`
                WHERE `account_id` IN %s;
            """, accounts)

    async def started(self, application):
        await super(EventsModel, self).started(application)

        self.schedule.start()

    async def stopped(self):
        await super(EventsModel, self).stopped()
        await self.schedule.stop()

    async def __get_leaderboard_top__(self, gamespace, event_id, clustered):
        leaderboard_name = EventAdapter.tournament_leaderboard_name(event_id, clustered)
        leaderboard_order = EventAdapter.tournament_leaderboard_order()

        try:
            top_entries = await self.internal.request(
                "leaderboard", "get_top_all_clusters",
                gamespace=gamespace, sort_order=leaderboard_order,
                leaderboard_name=leaderboard_name)
        except InternalError as e:
            if e.code == 404:
                logging.info("No such leaderboard: " + leaderboard_name)
            else:
                logging.exception("Failed to get leaderboard: " + str(e))

            return None
        else:
            return top_entries

    async def __post_score_to_leaderboard__(self, account, gamespace, score, event_id, clustered,
                                            display_name, expire_in, profile):
        leaderboard_name = EventAdapter.tournament_leaderboard_name(event_id, clustered)
        leaderboard_order = EventAdapter.tournament_leaderboard_order()

        try:
            await self.internal.request(
                "leaderboard", "post",
                account=account, gamespace=gamespace, sort_order=leaderboard_order,
                leaderboard_name=leaderboard_name, score=score, display_name=display_name,
                expire_in=expire_in, profile=profile)
        except InternalError as e:
            logging.exception("Failed to post to leaderboard: " + str(e))

    @validate(gamespace_id="int", event_id="int", account_id="int", score="float",
              leaderboard_info="json_dict", auto_join="bool")
    async def add_score(self, gamespace_id, event_id, account_id, score, leaderboard_info, auto_join=False):
        """
        Adds score to users record per event.
        :param gamespace_id: Current gamespace
        :param event_id: Event this score adds to
        :param account_id: User account
        :param score: Amount to add
        :param leaderboard_info: A dict will be passed to appropriate leaderboard in
                case the tournament is enabled
        :param auto_join: if True and no participation is registered, join automatically
        """

        async with self.db.acquire(auto_commit=False) as db:

            try:
                # lookup for existing participation along with some event information
                # current record is locked to avoid concurrency issues
                res = await db.get(
                    """
                        SELECT `participation_score`, (
                                SELECT CONCAT(`event_status`, '|', `event_flags`)
                                FROM `events` AS e
                                WHERE e.`event_id` = p.`event_id`
                            ) AS `event_status` FROM `event_participants` AS p
                        WHERE `event_id`=%s AND `account_id`=%s AND `gamespace_id`=%s AND `participation_status`=%s
                        LIMIT 1
                        FOR UPDATE;
                    """,
                    event_id, account_id, gamespace_id, "JOINED")

                if res:
                    # if there's a participation, check if the evens is active
                    event_status = res["event_status"]
                    active = False

                    if not event_status:
                        raise EventError("Bad event (not event_status)", code=500)

                    active, flags = event_status.split("|")
                    flags = EventFlags(flags.lower().split(","))

                    if EventFlags.GROUP in flags:
                        raise EventError("Event is group kind, and 'group_id' is not passed", code=409)

                    if active != EVENT_STATUS_ACTIVE:
                        raise EventError("Event is not active", code=409)

                    # get the existent score
                    old_score = res["participation_score"]
                else:
                    if auto_join:

                        await db.commit()

                        # if user has not been participated in this event, join him
                        await self.join_event(
                            gamespace_id, event_id, account_id, score=score,
                            leaderboard_info=leaderboard_info)

                        return score
                    else:
                        raise EventError("Event is not joined", code=406)

                # add the score from db to the posted score
                new_score = old_score + score

                # update the score, releasing the lock
                await db.execute(
                    """
                    UPDATE `event_participants`
                    SET `participation_score`=%s
                    WHERE `event_id`=%s AND `account_id`=%s AND `gamespace_id`=%s
                    LIMIT 1;
                    """, new_score, event_id, account_id, gamespace_id)

                # if there's a tournament attached to this event, post the score to the leaderboard
                if new_score and (EventFlags.TOURNAMENT in flags):
                    if leaderboard_info is None:
                        raise EventError("leaderboard_info is required", 400)

                    display_name = leaderboard_info.get("display_name")
                    expire_in = leaderboard_info.get("expire_in")
                    profile = leaderboard_info.get("profile", {})

                    if not display_name or not expire_in:
                        raise EventError("Cannot post score to tournament: "
                                         "leaderboard_info should have 'display_name' and 'expire_in' fields", 400)

                    clustered = EventFlags.CLUSTERED in flags

                    await self.__post_score_to_leaderboard__(
                        account_id, gamespace_id, new_score, event_id, clustered,
                        display_name, expire_in, profile)

            finally:
                await db.commit()

        return new_score

    @validate(gamespace_id="int", event_id="int", group_id="int", account_id="int", score="float",
              leaderboard_info="json_dict", auto_join="bool")
    async def add_group_score(self, gamespace_id, event_id, group_id, account_id, score, leaderboard_info=None,
                              auto_join=False):
        """
        Adds score to groups record per event.
        :param gamespace_id: Current gamespace
        :param event_id: Event this score adds to
        :param account_id: User account
        :param group_id: User group
        :param score: Amount to add
        :param leaderboard_info: A dict will be passed to appropriate leaderboard in
                case the tournament is enabled
        :param auto_join: if True and no participation is registered, join automatically
        """

        async with self.db.acquire(auto_commit=False) as db:

            try:
                # lookup for existing participation along with some event information
                # current record is locked to avoid concurrency issues
                res = await db.get(
                    """
                        SELECT g.`group_participation_score`, p.`participation_score`, 
                            p.`group_id` AS `participation_group_id`,
                            (
                                SELECT CONCAT(`event_status`, '|', `event_flags`)
                                FROM `events` AS e
                                WHERE e.`event_id` = g.`event_id`
                            ) AS `event_status`
                        FROM `event_group_participants` AS g
                        LEFT JOIN `event_participants` AS p ON (
                            p.`event_id`=%s AND p.`account_id`=%s
                        )
                        WHERE g.`event_id`=%s AND g.`group_id`=%s AND g.`gamespace_id`=%s AND 
                            g.`group_participation_status`=%s
                        LIMIT 1
                        FOR UPDATE;
                    """,
                    event_id, account_id, event_id, group_id, gamespace_id, "JOINED")

                if res:
                    # if there's a participation, check if the evens is active
                    event_status = res["event_status"]

                    if not event_status:
                        raise EventError("Bad event (not event_status)", code=500)

                    active, flags = event_status.split("|")
                    flags = EventFlags(flags.lower().split(","))

                    if EventFlags.GROUP not in flags:
                        raise EventError("Event is not 'group' kind", code=409)

                    if active != EVENT_STATUS_ACTIVE:
                        raise EventError("Event is not active", code=409)

                    account_in_group = await self.__check_account_in_group__(gamespace_id, group_id, account_id)

                    if not account_in_group:
                        raise EventError("Account is not participating in that group", code=409)

                    # get the existent score
                    old_score = res["group_participation_score"]
                else:
                    if auto_join:

                        await db.commit()

                        # if user has not been participated in this event, join him
                        await self.join_group_event(
                            gamespace_id, event_id, group_id, account_id, score=score,
                            leaderboard_info=leaderboard_info)

                        return score
                    else:
                        raise EventError("Event is not joined", code=406)

                participation_score = res["participation_score"]

                if participation_score:

                    participation_group_id = res["participation_group_id"]

                    if str(participation_group_id) == str(group_id):

                        # add the score from db to the posted score
                        new_score = participation_score + score

                        # update the score, releasing the lock
                        await db.execute(
                            """
                            UPDATE `event_participants`
                            SET `participation_score`=%s
                            WHERE `event_id`=%s AND `account_id`=%s AND `gamespace_id`=%s
                            LIMIT 1;
                            """, new_score, event_id, account_id, gamespace_id)
                    else:
                        # user originally participated in different group, update a group, resetting score
                        await db.execute(
                            """
                            UPDATE `event_participants`
                            SET `participation_score`=%s, `group_id`=%s
                            WHERE `event_id`=%s AND `account_id`=%s AND `gamespace_id`=%s
                            LIMIT 1;
                            """, score, group_id, event_id, account_id, gamespace_id)

                else:
                    await db.insert(
                        """
                        INSERT INTO `event_participants`
                        (`event_id`, `gamespace_id`, `account_id`, `group_id`, `participation_score`, 
                         `participation_status`, `participation_profile`) 
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                        """, event_id, gamespace_id, account_id, group_id, score, 'JOINED', "{}")

                # add the score from db to the posted score
                new_group_score = old_score + score

                # update the score, releasing the lock
                await db.execute(
                    """
                    UPDATE `event_group_participants`
                    SET `group_participation_score`=%s
                    WHERE `event_id`=%s AND `group_id`=%s AND `gamespace_id`=%s
                    LIMIT 1;
                    """, new_group_score, event_id, group_id, gamespace_id)

                # if there's a tournament attached to this event, post the score to the leaderboard
                if new_group_score and (EventFlags.TOURNAMENT in flags):
                    if leaderboard_info is None:
                        raise EventError("leaderboard_info is required", 400)

                    display_name = leaderboard_info.get("display_name")
                    expire_in = leaderboard_info.get("expire_in")
                    profile = leaderboard_info.get("profile", {})

                    if not display_name or not expire_in:
                        raise EventError("Cannot post score to tournament: "
                                         "leaderboard_info should have 'display_name' and 'expire_in' fields", 400)

                    clustered = EventFlags.CLUSTERED in flags

                    await self.__post_score_to_leaderboard__(
                        group_id, gamespace_id, new_group_score, event_id, clustered,
                        display_name, expire_in, profile)

            finally:
                await db.commit()

        return new_group_score

    @validate(gamespace_id="int", event_id="int", account_id="int", score="float",
              leaderboard_info="json_dict", auto_join="bool")
    async def update_score(self, gamespace_id, event_id, account_id, score, leaderboard_info, auto_join=False):
        """
        Updates user's score per event.
        :param gamespace_id: Current gamespace
        :param event_id: Event this score adds to
        :param account_id: User account
        :param score: A value to set
        :param leaderboard_info: A dict will be passed to appropriate leaderboard in
                case the tournament is enabled
        :param auto_join: if True and no participation is registered, join automatically
        """

        if not isinstance(score, float):
            raise EventError("Score is not a float")

        async with self.db.acquire() as db:

            # lookup for event information
            event = await self.get_event(gamespace_id, event_id, db=db)

            if event.group:
                raise EventError("Event is 'group' kind, and not 'group_id' is passed", code=400)

            if not event.is_active():
                raise EventError("Event is not active!")

            participation = await db.get(
                """
                    SELECT 1 
                    FROM `event_participants`
                    WHERE `gamespace_id`=%s AND `event_id`=%s AND `account_id`=%s
                    LIMIT 1;
                """, gamespace_id, event_id, account_id)

            if participation:
                await db.execute(
                    """
                        UPDATE `event_participants`
                        SET `participation_score`=%s
                        WHERE `gamespace_id`=%s AND `event_id`=%s AND `account_id`=%s
                        LIMIT 1;
                    """, score, gamespace_id, event_id, account_id)
            else:
                if not auto_join:
                    raise EventError("Event is not joined", code=406)

                await self.join_event(
                    gamespace_id, event_id, account_id, score=score,
                    leaderboard_info=leaderboard_info)

            if event.tournament and leaderboard_info:

                display_name = leaderboard_info.get("display_name")
                expire_in = leaderboard_info.get("expire_in")
                profile = leaderboard_info.get("profile", {})

                if not display_name or not expire_in:
                    raise EventError("Cannot post score to tournament: "
                                     "leaderboard_info should have 'display_name' and 'expire_in' fields",
                                     400)

                await self.__post_score_to_leaderboard__(
                    account_id, gamespace_id, score, event_id, event.clustered,
                    display_name, expire_in, profile)

        return score

    @validate(gamespace_id="int", event_id="int", group_id="int", account_id="int", score="float",
              leaderboard_info="json_dict", auto_join="bool")
    async def update_group_score(self, gamespace_id, event_id, group_id, account_id, score,
                                 leaderboard_info, auto_join=False):
        """
        Updates user's score per event.
        :param gamespace_id: Current gamespace
        :param event_id: Event this score adds to
        :param group_id: User group
        :param account_id: User account
        :param score: A value to set
        :param leaderboard_info: A dict will be passed to appropriate leaderboard in
                case the tournament is enabled
        :param auto_join: if True and no participation is registered, join automatically
        """

        if not isinstance(score, float):
            raise EventError("Score is not a float")

        async with self.db.acquire() as db:

            # lookup for event information
            event = await self.get_event(gamespace_id, event_id, db=db)

            if not event.group:
                raise EventError("Event is not 'group' kind, but 'group_id' is passed", code=400)

            if not event.is_active():
                raise EventError("Event is not active!")

            account_in_group = await self.__check_account_in_group__(gamespace_id, group_id, account_id)

            if not account_in_group:
                raise EventError("Account is not participating in that group", code=409)

            group_participation = await db.get(
                """
                    SELECT 1 
                    FROM `event_group_participants`
                    WHERE `gamespace_id`=%s AND `event_id`=%s AND `group_id`=%s
                    LIMIT 1;
                """, gamespace_id, event_id, group_id)

            if group_participation:
                await db.execute(
                    """
                        UPDATE `event_group_participants`
                        SET `group_participation_score`=%s
                        WHERE `gamespace_id`=%s AND `event_id`=%s AND `group_id`=%s
                        LIMIT 1;
                    """, score, gamespace_id, event_id, group_id)
            else:
                if not auto_join:
                    raise EventError("Event is not joined", code=406)

                await self.join_event(
                    gamespace_id, event_id, account_id, score=score,
                    leaderboard_info=leaderboard_info)

            if event.tournament and leaderboard_info:

                display_name = leaderboard_info.get("display_name")
                expire_in = leaderboard_info.get("expire_in")
                profile = leaderboard_info.get("profile", {})

                if not display_name or not expire_in:
                    raise EventError("Cannot post score to tournament: "
                                     "leaderboard_info should have 'display_name' and 'expire_in' fields",
                                     400)

                await self.__post_score_to_leaderboard__(
                    group_id, gamespace_id, score, event_id, event.clustered,
                    display_name, expire_in, profile)

        return score

    @validate(gamespace_id="int", category_id="int")
    async def clone_category_scheme(self, gamespace_id, category_id):
        logging.debug("Cloning event category '%s'", category_id)
        category = await self.get_category(gamespace_id, category_id)

        category_scheme = category.scheme

        if 'title' in category_scheme:
            category_scheme['title'] = 'Clone of ' + category_scheme['title']

        result = await self.db.insert(
            """
                INSERT INTO `category_scheme` (`gamespace_id`, `scheme_json`)
                SELECT `gamespace_id`, %s
                    FROM `category_scheme`
                    WHERE `id`=%s AND `gamespace_id`=%s
            """,
            ujson.dumps(category_scheme), category_id, gamespace_id)

        return result

    @validate(gamespace_id="int", category_name="str", scheme="json_dict")
    async def create_category(self, gamespace_id, category_name, scheme):
        result = await self.db.insert(
            """
                INSERT INTO `category_scheme`
                (`gamespace_id`, `category_name`, `scheme_json`)
                VALUES
                (%s, %s, %s);
            """,
            gamespace_id, category_name, ujson.dumps(scheme))

        return result

    @validate(gamespace_id="int", category_id="int", enabled="bool", float=EventFlags,
              start_dt="datetime", end_dt="datetime", end_action=EventEndAction)
    async def create_event(self, gamespace_id, category_id, enabled, flags, payload, start_dt, end_dt, end_action):

        category = await self.get_category(gamespace_id, category_id)

        event_id = await self.db.insert(
            """
                INSERT INTO `events`
                (`gamespace_id`, `category_id`, `event_enabled`, `event_status`, `event_flags`, `category_name`,
                    `event_payload`, `event_start_dt`, `event_end_dt`, `event_end_action`)
                VALUES
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            gamespace_id, category_id, int(enabled), EVENT_STATUS_NOT_STARTED,
            flags.dump(), category.name, ujson.dumps(payload),
            start_dt, end_dt, str(end_action))

        return event_id

    @validate(gamespace_id="int", category_id="int")
    async def delete_category(self, gamespace_id, category_id):

        await self.db.execute(
            """
                DELETE FROM `events`
                WHERE `category_id`=%s AND `gamespace_id`=%s
            """,
            category_id, gamespace_id)

        await self.db.execute(
            """
                DELETE FROM `category_scheme`
                WHERE `id`=%s AND `gamespace_id`=%s
                LIMIT 1;
            """,
            category_id, gamespace_id)

    @validate(gamespace_id="int", event_id="int")
    async def delete_event(self, gamespace_id, event_id):
        # find the event, EventNotFound otherwise
        event = await self.get_event(gamespace_id, event_id)

        await self.db.execute(
            """
                DELETE FROM `event_participants`
                WHERE `event_id`=%s AND `gamespace_id`=%s;
            """,
            event_id, gamespace_id)

        await self.db.execute(
            """
                DELETE FROM `events`
                WHERE `event_id`=%s AND `gamespace_id`=%s
                LIMIT 1;
            """,
            event_id, gamespace_id)

        if event.tournament:
            await self.__delete_leaderboard__(event_id, gamespace_id, event.clustered)

    @validate(gamespace_id="int", category_id="int")
    async def get_category(self, gamespace_id, category_id):
        category = await self.db.get(
            """
                SELECT *
                FROM category_scheme
                WHERE `id` = %s AND `gamespace_id` = %s
                LIMIT 1;
            """,
            category_id, gamespace_id)

        if not category:
            raise CategoryNotFound()

        return CategoryAdapter(category)

    @validate(gamespace_id="int", event_id="int")
    async def list_event_participants(self, gamespace_id, event_id):
        participants = await self.db.query(
            """
                SELECT *
                FROM `event_participants`
                WHERE `event_id`=%s AND `gamespace_id`=%s AND `participation_status`='JOINED';
            """, event_id, gamespace_id)

        result = {
            int(participant["account_id"]): EventParticipationAdapter(participant)
            for participant in participants
        }

        return result

    @validate(gamespace_id="int", event_id="int")
    async def update_event_group_participants_tournament_result(self, gamespace_id, event_id, participants, ranks):

        data = []
        values = []

        for group_id, participant in participants.items():
            rank = ranks.get(str(group_id), None)

            if rank is None:
                continue

            values.append("(%s, %s, %s, %s, %s)")
            data.extend([
                event_id, gamespace_id, participant.group_id, "{}", rank
            ])

        await self.db.execute(
            """
                INSERT INTO `event_group_participants`
                (`event_id`, `gamespace_id`, `group_id`, `group_participation_profile`, `group_participation_tournament_result`) 
                VALUES {0}
                ON DUPLICATE KEY UPDATE 
                `group_participation_tournament_result`=VALUES(`group_participation_tournament_result`);
            """.format(",".join(values)), *data)

    @validate(gamespace_id="int", event_id="int")
    async def update_event_participants_tournament_result(self, gamespace_id, event_id, participants, ranks):

        data = []
        values = []

        for account_id, participant in participants.items():
            rank = ranks.get(str(account_id), None)

            if rank is None:
                continue

            values.append("(%s, %s, %s, %s, %s)")
            data.extend([
                event_id, gamespace_id, participant.account_id, "{}", rank
            ])

        await self.db.execute(
            """
                INSERT INTO `event_participants`
                (`event_id`, `gamespace_id`, `account_id`, `participation_profile`, `participation_tournament_result`) 
                VALUES {0}
                ON DUPLICATE KEY UPDATE 
                `participation_tournament_result`=VALUES(`participation_tournament_result`);
            """.format(",".join(values)), *data)

    @validate(gamespace_id="int", event_id="int")
    async def list_event_group_participants(self, gamespace_id, event_id):
        participants = await self.db.query(
            """
                SELECT *
                FROM `event_group_participants`
                WHERE `event_id`=%s AND `gamespace_id`=%s AND `group_participation_status`='JOINED';
            """, event_id, gamespace_id)

        result = {
            int(participant["group_id"]): GroupParticipationAdapter(participant)
            for participant in participants
        }

        return result

    @validate(gamespace_id="int", event_id="int", group_id="int")
    async def list_group_account_participants(self, gamespace_id, event_id, group_id):
        participants = await self.db.query(
            """
                SELECT *
                FROM `event_participants`
                WHERE `event_id`=%s AND `gamespace_id`=%s AND `group_id`=%s 
                    AND `participation_status`='JOINED';
            """, event_id, gamespace_id, group_id)

        result = {
            int(participant["account_id"]): EventParticipationAdapter(participant)
            for participant in participants
        }

        return result

    @validate(gamespace_id="int")
    async def get_common_scheme(self, gamespace_id):
        common_scheme = await self.db.get(
            """
                SELECT `scheme_json`
                FROM `common_scheme`
                WHERE `gamespace_id`=%s
                LIMIT 1;
            """,
            gamespace_id)

        if not common_scheme:
            return {}

        common_scheme = common_scheme['scheme_json']
        return common_scheme

    @validate(gamespace_id="int", event_id="int")
    async def get_event(self, gamespace_id, event_id, db=None):

        event = await (db or self.db).get(
            """
                SELECT *
                FROM `events`
                WHERE `event_id`=%s AND `gamespace_id`=%s
                LIMIT 1;
            """,
            event_id, gamespace_id)

        if event:
            return EventAdapter(event)

        raise EventNotFound()

    @validate(gamespace_id="int")
    async def list_categories(self, gamespace_id):
        categories = await self.db.query(
            """
                SELECT *
                FROM category_scheme
                WHERE `gamespace_id`=%s;
            """,
            gamespace_id)

        return list(map(CategoryAdapter, categories))

    @validate(gamespace_id="int", account_id="int", group_id="int", extra_start_time="int", extra_end_time="int")
    async def list_events(self, gamespace_id, account_id, group_id=0, extra_start_time=0, extra_end_time=0):

        dt = datetime.datetime.fromtimestamp(utc_time(), tz=pytz.utc).strftime('%Y-%m-%d %H:%M:%S')

        events = await self.db.query(
            """
                SELECT `events`.*,
                    `participant`.`account_id`, `participant`.`participation_status`, 
                    `participant`.`participation_score`, `participant`.`participation_profile`,
                    `participant`.`participation_tournament_result`,
                    
                    `group_participant`.`group_id`, `group_participant`.`group_participation_status`, 
                    `group_participant`.`group_participation_score`, 
                    `group_participant`.`group_participation_profile`,
                    `group_participant`.`group_participation_tournament_result`
                FROM `category_scheme`
                JOIN `events` ON `category_scheme`.id = `events`.category_id
                LEFT JOIN (
                     SELECT * 
                     FROM `event_participants`
                     WHERE `account_id`=%s
                ) AS `participant` ON 
                    (
                        `events`.`event_id`=`participant`.`event_id` AND
                        FIND_IN_SET('GROUP', `events`.`event_flags`) = 0 
                    )
                LEFT JOIN (
                     SELECT * 
                     FROM `event_group_participants` 
                     WHERE `group_id`=%s
                ) AS `group_participant` ON 
                    (
                        `events`.`event_id`=`group_participant`.`event_id` AND
                        FIND_IN_SET('GROUP', `events`.`event_flags`) > 0 
                    )
                WHERE
                    `category_scheme`.`gamespace_id` = %s AND
                    %s BETWEEN 
                        DATE_SUB(`events`.`event_start_dt`, INTERVAL %s second) AND 
                        DATE_ADD(`events`.`event_end_dt`, INTERVAL %s second);
            """,
            account_id, group_id, gamespace_id, dt, extra_start_time, extra_end_time)

        return [EventWithParticipationAdapter(event) for event in events]

    @validate(gamespace_id="int", event_id="int", account_id="int", score="float",
              leaderboard_info="json_dict", group_id="int")
    async def join_event(self, gamespace_id, event_id, account_id,
                         score=0.0, leaderboard_info=None):

        async with self.db.acquire() as db:
            event_data = await db.get(
                """
                    SELECT *
                    FROM `events`
                    WHERE `event_id`=%s AND `gamespace_id`=%s
                    LIMIT 1;
                """,
                event_id, gamespace_id)

            if not event_data:
                raise EventNotFound()

            event = EventAdapter(event_data)

            if event.group:
                raise EventError("Event is a group type, 'group_id' should be passed", code=409)

            if not event.is_active():
                raise EventError("Event is not active")

            try:
                await db.insert(
                    """
                        INSERT INTO `event_participants`
                        (`account_id`, `gamespace_id`, `event_id`, 
                         `participation_status`, `participation_score`, `participation_profile`)
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

                    await self.__post_score_to_leaderboard__(
                        account_id, gamespace_id, score, event_id, event.clustered,
                        display_name, expire_in, profile)

                return event

            except DuplicateError:
                raise EventError("The user already took part in the event", code=409)

    async def __check_account_in_group__(self, gamespace_id, group_id, account_id):

        @cached(kv=self.app.cache,
                h="group_check:" + str(gamespace_id) + ":" + str(group_id),
                json=True,
                ttl=300,
                check_is_cached=True)
        async def _check():
            try:
                result = await self.internal.request(
                    "social",
                    "get_group",
                    gamespace=gamespace_id,
                    group_id=group_id)

            except InternalError as e:
                raise EventError(e.body, e.code)

            return result

        group_data, is_cached = await _check()
        participants = group_data.get("participants", {})

        if str(account_id) in participants:
            return True

        if not is_cached:
            return False

        # is there's no participant and the value was cached there is a probability that cache
        #    is simply outdated, so the request should be tried again ignoring the cache

        try:
            group_data = await self.internal.request(
                "social",
                "get_group",
                gamespace=gamespace_id,
                group_id=group_id)

        except InternalError as e:
            raise EventError(e.body, e.code)

        participants = group_data.get("participants", {})
        return str(account_id) in participants

    @validate(gamespace_id="int", event_id="int", group_id="int", account_id="int", score="float",
              leaderboard_info="json_dict")
    async def join_group_event(self, gamespace_id, event_id, group_id, account_id, score=0.0, leaderboard_info=None):
        async with self.db.acquire() as db:
            event_data = await db.get(
                """
                    SELECT *
                    FROM `events`
                    WHERE `event_id`=%s AND `gamespace_id`=%s
                    LIMIT 1;
                """,
                event_id, gamespace_id)

            if not event_data:
                raise EventNotFound()

            event = EventAdapter(event_data)

            if not event.group:
                raise EventError("Event is not a group type", code=409)

            if not event.is_active():
                raise EventError("Event is not active")

            account_in_group = await self.__check_account_in_group__(gamespace_id, group_id, account_id)

            if not account_in_group:
                raise EventError("Account is not participating in that group", code=409)

            try:
                await db.insert(
                    """
                        INSERT INTO `event_group_participants`
                        (`group_id`, `gamespace_id`, `event_id`, 
                         `group_participation_status`, `group_participation_score`, `group_participation_profile`)
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """,
                    group_id, gamespace_id, event_id, "JOINED", score, "{}")

                if event.tournament and leaderboard_info:

                    display_name = leaderboard_info.get("display_name")
                    expire_in = leaderboard_info.get("expire_in")
                    profile = leaderboard_info.get("profile", {})

                    if not display_name or not expire_in:
                        raise EventError("Cannot post score to tournament: "
                                         "leaderboard_info should have 'display_name' and 'expire_in' fields", 400)

                    await self.__post_score_to_leaderboard__(
                        group_id, gamespace_id, score, event_id, event.clustered,
                        display_name, expire_in, profile)

                return event

            except DuplicateError:
                raise EventError("This group has already took part in the event", code=409)

    @validate(gamespace_id="int", event_id="int", account_id="int", group_id="int")
    async def leave_event(self, gamespace_id, event_id, account_id, group_id=0):

        event = await self.get_event(gamespace_id, event_id)

        if event.group:
            if not group_id:
                raise EventError("Event is a 'group' kind and 'group_id' is omitted.")

            account_in_group = await self.__check_account_in_group__(gamespace_id, group_id, account_id)

            if not account_in_group:
                raise EventError("Account is not participating in that group", code=409)

            result = await self.db.execute(
                """
                    UPDATE `event_group_participants`
                    SET `group_participation_status`= %s
                    WHERE `event_id`=%s AND `group_id`=%s AND `gamespace_id`=%s
                    LIMIT 1;
                """,
                "LEFT", group_id, account_id, gamespace_id)
        else:
            result = await self.db.execute(
                """
                    UPDATE `event_participants`
                    SET `participation_status`= %s
                    WHERE `event_id`=%s AND `account_id`=%s AND `gamespace_id`=%s
                    LIMIT 1;
                """,
                "LEFT", event_id, account_id, gamespace_id)

        if not result:
            raise EventError(
                "Either the event doesn't exist or the user doesn't participate in it")

        return result

    @validate(gamespace_id="int", category_id="int", items_in_page="int", page="int")
    async def list_paged_events(self, gamespace_id, items_in_page, page, category_id=None):

        filters = []
        params = []

        if category_id and int(category_id):
            filters.append("AND category_id=%s")
            params.append(category_id)

        async with self.db.acquire(auto_commit=False) as db:
            import math
            page = max(page, 1)

            limit_a = (page - 1) * items_in_page
            limit_b = page * items_in_page

            params += [limit_a, limit_b]

            events = await db.query(
                """
                    SELECT SQL_CALC_FOUND_ROWS *
                    FROM `events`
                    WHERE gamespace_id=%s {0}
                    ORDER BY `event_start_dt` ASC
                    LIMIT %s, %s;
                """.format("".join(filters)), gamespace_id, *params)

            rows = await db.get(
                """
                    SELECT FOUND_ROWS() AS count;
                """)

            pages = int(math.ceil(float(rows["count"]) / float(items_in_page)))

            result = [EventAdapter(event) for event in events], pages
            return result

    @validate(gamespace_id="int", category_id="int", new_scheme="json_dict", category_name="str")
    async def update_category(self, gamespace_id, category_id, new_scheme, category_name):

        await self.db.execute(
            """
                UPDATE `category_scheme`
                SET `scheme_json`=%s, `category_name`=%s
                WHERE `id`=%s AND `gamespace_id`=%s
                LIMIT 1;
            """,
            ujson.dumps(new_scheme), category_name, category_id, gamespace_id)

        await self.db.execute(
            """
                UPDATE `events`
                SET `category_name`=%s
                WHERE `category_id`=%s AND `gamespace_id`=%s;
            """,
            category_name, category_id, gamespace_id)

    @validate(gamespace_id="int", new_scheme="json_dict")
    async def update_common_scheme(self, gamespace_id, new_scheme):

        await self.db.insert(
            """
                INSERT INTO common_scheme
                (`gamespace_id`, `scheme_json`) 
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE 
                `scheme_json`=VALUES(`scheme_json`);
            """,
            gamespace_id, ujson.dumps(new_scheme))

    @validate(gamespace_id="int", event_id="int", enabled="bool", flags=EventFlags, payload="json_dict",
              start_dt="datetime", end_dt="datetime", end_action=EventEndAction)
    async def update_event(self, gamespace_id, event_id, enabled, flags, payload, start_dt, end_dt, end_action):
        result = await self.db.execute(
            """
                UPDATE `events`
                SET `event_payload`=%s, `event_start_dt`=%s, `event_end_dt`=%s, 
                    `event_enabled`=%s, `event_flags`=%s, `event_end_action`=%s
                WHERE `event_id`=%s AND `gamespace_id`=%s
                LIMIT 1;
            """,
            ujson.dumps(payload), start_dt, end_dt, int(enabled),
            flags.dump(), str(end_action), event_id, gamespace_id)

        return result

    @validate(gamespace_id="int", event_id="int", account_id="int", profile="json_dict",
              path="json_list_of_strings", merge="bool")
    async def update_profile(self, gamespace_id, event_id, account_id, profile, path=None, merge=True):
        profile_obj = ParticipationProfile(self.db, gamespace_id, event_id, account_id)

        try:
            result = await profile_obj.set_data(profile, path, merge=merge)
        except NoDataError:
            raise EventError("User is not participating in the event")
        except ProfileError as e:
            raise EventError("Failed to update event profile: " + e.message)

        return result

    @validate(gamespace_id="int", event_id="int", group_id="int", profile="json_dict",
              path="json_list_of_strings", merge="bool")
    async def update_group_profile(self, gamespace_id, event_id, group_id, profile, path=None, merge=True):
        profile_obj = GroupParticipationProfile(self.db, gamespace_id, event_id, group_id)
        try:
            result = await profile_obj.set_data(profile, path, merge=merge)
        except NoDataError:
            raise EventError("Group is not participating in the event")
        except ProfileError as e:
            raise EventError("Failed to update event profile: " + e.message)
        return result

    @validate(gamespace_id="int", event_id="int", account_id="int", path="json_list_of_strings")
    async def get_profile(self, gamespace_id, event_id, account_id, path=None):
        profile_obj = ParticipationProfile(self.db, gamespace_id, event_id, account_id)
        try:
            result = await profile_obj.get_data(path)
        except NoDataError:
            raise EventError("Player is not participating in the event")
        except ProfileError as e:
            raise EventError("Failed to get profile: " + e.message)

        return result

    @validate(gamespace_id="int", event_id="int", group_id="int", path="json_list_of_strings")
    async def get_group_profile(self, gamespace_id, event_id, group_id, path=None):
        profile_obj = GroupParticipationProfile(self.db, gamespace_id, event_id, group_id)
        try:
            result = await profile_obj.get_data(path)
        except NoDataError:
            raise EventError("Group is not participating in the event")
        except ProfileError as e:
            raise EventError("Failed to update event profile: " + e.message)
        return result


class ParticipationProfile(profile.DatabaseProfile):
    def __init__(self, db, gamespace_id, event_id, account_id):
        super(ParticipationProfile, self).__init__(db)
        self.gamespace_id = gamespace_id
        self.event_id = event_id
        self.account_id = account_id

    async def get(self):
        result = await self.conn.get(
            """
            SELECT `participation_profile` FROM `event_participants`
            WHERE `event_id`=%s AND `account_id`=%s AND `gamespace_id`=%s
            LIMIT 1
            FOR UPDATE;
            """, self.event_id, self.account_id, self.gamespace_id)

        if result:
            return result["participation_profile"]

        raise NoDataError()

    async def insert(self, data):
        raise ProfileError("Insert is not supported")

    def update(self, data):
        return self.conn.execute(
            """
            UPDATE `event_participants`
            SET `participation_profile`=%s
            WHERE `event_id`=%s AND `account_id`=%s AND `gamespace_id`=%s
            LIMIT 1;
            """, ujson.dumps(data), self.event_id, self.account_id, self.gamespace_id)


class GroupParticipationProfile(profile.DatabaseProfile):
    def __init__(self, db, gamespace_id, event_id, group_id):
        super(GroupParticipationProfile, self).__init__(db)
        self.gamespace_id = gamespace_id
        self.event_id = event_id
        self.group_id = group_id

    async def get(self):
        result = await self.conn.get(
            """
            SELECT `group_participation_profile` FROM `event_group_participants`
            WHERE `event_id`=%s AND `group_id`=%s AND `gamespace_id`=%s
            LIMIT 1
            FOR UPDATE;
            """, self.event_id, self.group_id, self.gamespace_id)

        if result:
            return result["group_participation_profile"]

        raise NoDataError()

    async def insert(self, data):
        raise ProfileError("Insert is not supported")

    def update(self, data):
        return self.conn.execute(
            """
            UPDATE `event_group_participants`
            SET `group_participation_profile`=%s
            WHERE `event_id`=%s AND `group_id`=%s AND `gamespace_id`=%s
            LIMIT 1;
            """, ujson.dumps(data), self.event_id, self.group_id, self.gamespace_id)
