
from tornado.web import HTTPError

from anthill.common.handler import AuthenticatedHandler
from anthill.common.access import scoped, AccessToken
from anthill.common.validate import validate

from . model.event import EventNotFound, EventError

import logging
import traceback
import ujson


class EventJoinHandler(AuthenticatedHandler):
    @scoped(scopes=["event_join"])
    async def post(self, event_id):
        token = self.token
        account_id = token.account
        gamespace_id = token.get(AccessToken.GAMESPACE)

        score = self.get_argument("score", 0.0)
        leaderboard_info = self.get_argument("leaderboard_info", None)
        if leaderboard_info:
            try:
                leaderboard_info = ujson.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "json 'leaderboard_info' is corrupted")

        try:
            await self.application.events.join_event(
                gamespace_id, event_id, account_id,
                score=score, leaderboard_info=leaderboard_info)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500,
                "Failed to engage user '{0}' in the event '{1}': {2}".format(
                    account_id, event_id, e))


class EventGroupJoinHandler(AuthenticatedHandler):
    @scoped(scopes=["event_join"])
    async def post(self, event_id):
        token = self.token
        account_id = token.account
        gamespace_id = token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        score = self.get_argument("score", 0.0)
        leaderboard_info = self.get_argument("leaderboard_info", None)
        if leaderboard_info:
            try:
                leaderboard_info = ujson.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "json 'leaderboard_info' is corrupted")

        try:
            await self.application.events.join_group_event(
                gamespace_id, event_id, group_id, account_id,
                score=score, leaderboard_info=leaderboard_info)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500,
                "Failed to engage user '{0}' in the event '{1}': {2}".format(
                    account_id, event_id, e))


class EventLeaveHandler(AuthenticatedHandler):
    @scoped(scopes=["event_leave"])
    async def post(self, event_id):
        account_id = self.current_user.token.account
        gamespace_id = self.current_user.token.get(AccessToken.GAMESPACE)

        try:
            await self.application.events.leave_event(
                gamespace_id, event_id, account_id)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500,
                "Failed to make user '{0}' to leave the event '{1}': {2}".format(
                    account_id, event_id, e))


class EventGroupLeaveHandler(AuthenticatedHandler):
    @scoped(scopes=["event_leave"])
    async def post(self, event_id):
        account_id = self.current_user.token.account
        gamespace_id = self.current_user.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        try:
            await self.application.events.leave_event(
                gamespace_id, event_id, account_id, group_id=group_id)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500,
                "Failed to make user '{0}' to leave the event '{1}': {2}".format(
                    account_id, event_id, e))


class EventProfileHandler(AuthenticatedHandler):
    @scoped()
    async def get(self, event_id):
        events = self.application.events

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        path = self.get_argument("path", None)
        if path:
            path = list(filter(bool, path.split("/")))

        try:
            data = await events.get_profile(
                gamespace_id, event_id, account_id, path=path)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500, "Failed to update profile for "
                     "the user '{0}' in the event '{1}': {2}".format(account_id, event_id, e))

        self.dumps(data)

    @scoped(scopes=["event_profile_write"])
    async def post(self, event_id):
        events = self.application.events

        merge = self.get_argument("merge", "true") == "true"
        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        path = self.get_argument("path", None)
        if path:
            path = list(filter(bool, path.split("/")))

        try:
            profile = ujson.loads(self.get_argument("profile"))
        except Exception:
            raise HTTPError(400, "Not a valid profile.")

        try:
            new_data = await events.update_profile(
                gamespace_id, event_id, account_id,
                profile, path=path, merge=merge)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500, "Failed to update profile for "
                     "the user '{0}' in the event '{1}': {2}".format(account_id, event_id, e))

        self.dumps(new_data)


class EventGroupProfileHandler(AuthenticatedHandler):
    @scoped(scopes=[])
    async def get(self, event_id):
        events = self.application.events

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        path = self.get_argument("path", None)
        if path:
            path = list(filter(bool, path.split("/")))

        try:
            data = await events.get_group_profile(
                gamespace_id, event_id, group_id, path=path)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500, "Failed to get group profile for "
                     "the user '{0}' in the event '{1}': {2}".format(account_id, event_id, e))

        self.dumps(data)

    @scoped(scopes=["event_profile_write"])
    async def post(self, event_id):
        events = self.application.events

        merge = self.get_argument("merge", "true") == "true"
        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        path_str = self.get_argument("path", None)
        path = list(filter(bool, path_str.split("/"))) if path_str else []

        try:
            group_profile = ujson.loads(self.get_argument("group_profile"))
        except Exception:
            raise HTTPError(400, "Not a valid 'group_profile'.")

        try:
            new_data = await events.update_group_profile(
                gamespace_id, event_id, group_id,
                group_profile, path=path, merge=merge)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500, "Failed to update group profile for "
                     "the user '{0}' in the event '{1}': {2}".format(account_id, event_id, e))

        self.dumps(new_data)


class EventGroupParticipantsHandler(AuthenticatedHandler):
    @scoped(scopes=[])
    async def get(self, event_id):
        events = self.application.events

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        try:
            participants = await events.list_group_account_participants(
                gamespace_id, event_id, group_id)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500, "Failed to get group profile for "
                     "the user '{0}' in the event '{1}': {2}".format(account_id, event_id, e))

        self.dumps({
            "participants": {
                account_id: {
                    "profile": participant.profile,
                    "score": participant.score
                }
                for account_id, participant in participants.items()
            }
        })

    @scoped(scopes=["event_profile_write"])
    async def post(self, event_id):
        events = self.application.events

        path_str = self.get_argument("path", None)
        path = list(filter(bool, path_str.split("/"))) if path_str else []

        merge = self.get_argument("merge", "true") == "true"
        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        try:
            group_profile = ujson.loads(self.get_argument("group_profile"))
        except Exception:
            raise HTTPError(400, "Not a valid 'group_profile'.")

        try:
            new_data = await events.update_group_profile(
                gamespace_id, event_id, group_id,
                group_profile, path, merge)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500, "Failed to update group profile for "
                     "the user '{0}' in the event '{1}': {2}".format(account_id, event_id, e))

        self.dumps(new_data)


class EventAddScoreHandler(AuthenticatedHandler):
    @scoped(scopes=["event_write"])
    async def post(self, event_id):

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        auto_join = self.get_argument("auto_join", "true") == "true"
        score = self.get_argument("score")

        if auto_join and not self.has_scopes(["event_join"]):
            raise HTTPError(403, "Scope 'event_join' is required for auto_join")

        leaderboard_info = self.get_argument("leaderboard_info", None)
        if leaderboard_info:
            try:
                leaderboard_info = ujson.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "json 'leaderboard_info' is corrupted")

        try:
            new_score = await self.application.events.add_score(
                gamespace_id, event_id, account_id,
                score, leaderboard_info, auto_join=auto_join)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            raise HTTPError(
                500, "Failed to update score for the user '{0}' in the event '{1}': {2}".format(
                    account_id, event_id, e))

        self.dumps({
            "score": new_score
        })


class EventGroupAddScoreHandler(AuthenticatedHandler):
    @scoped(scopes=["event_write"])
    async def post(self, event_id):

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        group_id = self.get_argument("group_id")
        auto_join = self.get_argument("auto_join", "true") == "true"
        score = self.get_argument("score")

        if auto_join and not self.has_scopes(["event_join"]):
            raise HTTPError(403, "Scope 'event_join' is required for auto_join")

        leaderboard_info = self.get_argument("leaderboard_info", None)
        if leaderboard_info:
            try:
                leaderboard_info = ujson.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "json 'leaderboard_info' is corrupted")

        try:
            new_score = await self.application.events.add_group_score(
                gamespace_id, event_id, group_id, account_id,
                score, leaderboard_info=leaderboard_info, auto_join=auto_join)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            raise HTTPError(
                500, "Failed to update score for the user '{0}' in the event '{1}': {2}".format(
                    account_id, event_id, e))

        self.dumps({
            "score": new_score
        })


class EventUpdateScoreHandler(AuthenticatedHandler):
    @scoped(scopes=["event_write"])
    async def post(self, event_id):
        score = self.get_argument("score")

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        auto_join = self.get_argument("auto_join", "true") == "true"

        if auto_join and not self.has_scopes(["event_join"]):
            raise HTTPError(403, "Scope 'event_join' is required for auto_join")

        leaderboard_info = self.get_argument("leaderboard_info")
        if leaderboard_info:
            try:
                leaderboard_info = ujson.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "json 'leaderboard_info' is corrupted")

        try:
            new_score = await self.application.events.update_score(
                gamespace_id, event_id, account_id,
                score, leaderboard_info)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404,  "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500, "Failed to update score for "
                     "the user '{0}' in the event '{1}': {2}".format(account_id, event_id, e))
        else:
            self.dumps({
                "score": new_score
            })


class EventGroupUpdateScoreHandler(AuthenticatedHandler):
    @scoped(scopes=["event_write"])
    async def post(self, event_id):
        score = self.get_argument("score")

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        group_id = self.get_argument("group_id")
        auto_join = self.get_argument("auto_join", "true") == "true"

        if auto_join and not self.has_scopes(["event_join"]):
            raise HTTPError(403, "Scope 'event_join' is required for auto_join")

        leaderboard_info = self.get_argument("leaderboard_info")
        if leaderboard_info:
            try:
                leaderboard_info = ujson.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "json 'leaderboard_info' is corrupted")

        try:
            new_score = await self.application.events.update_group_score(
                gamespace_id, event_id, group_id, account_id,
                score, leaderboard_info, auto_join=auto_join)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404,  "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500, "Failed to update score for "
                     "the group '{0}' in the event '{1}': {2}".format(group_id, event_id, e))
        else:
            self.dumps({
                "score": new_score
            })


class EventsHandler(AuthenticatedHandler):
    @scoped()
    async def get(self):

        events = self.application.events
        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        group_id = self.get_argument("group_id", 0)

        extra_start_time = self.get_argument("extra_start_time", 0)
        extra_end_time = self.get_argument("extra_end_time", self.get_argument("extra_time", 0))

        try:
            events_list = await events.list_events(
                gamespace_id, account_id,
                group_id=group_id,

                extra_start_time=extra_start_time,
                extra_end_time=extra_end_time)

        except Exception as e:
            raise HTTPError(
                500, "Failed to fetch a list of "
                "current events available for user '{0}': {1}".format(account_id, e))
        else:
            self.dumps({
                "events": [
                    event.dump()
                    for event in events_list
                ]
            })


class InternalHandler(object):
    def __init__(self, application):
        self.application = application

    @validate(gamespace="int", event_id="int", account="int", path="str", profile="json", merge="bool")
    async def update_event_profile(self, gamespace, event_id, account, profile, path=None, merge=True):
        events = self.application.events

        if path:
            path = list(filter(bool, path.split("/")))

        try:
            new_data = await events.update_profile(
                gamespace, event_id, account,
                profile, path=path, merge=merge)
        except EventError as e:
            raise HTTPError(e.code, str(e))
        except EventNotFound:
            raise HTTPError(404, "Event '%s' was not found." % event_id)
        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500, "Failed to update profile for "
                     "the user '{0}' in the event '{1}': {2}".format(account, event_id, e))

        return new_data

    @validate(gamespace="int", account="int")
    async def get_list(self, gamespace, account, group=0, extra_start_time=0, extra_end_time=0, extra_time=0):
        events = self.application.events

        try:
            events_list = await events.list_events(
                gamespace, account,
                group_id=group,
                extra_start_time=extra_start_time,
                extra_end_time=extra_end_time or extra_time
            )

        except Exception as e:
            raise HTTPError(
                500, "Failed to fetch a list of "
                "current events available for user '{0}': {1}".format(account, e))
        else:
            return [
                event.dump()
                for event in events_list
            ]
