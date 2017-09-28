
import logging
import traceback
import json

from common.handler import AuthenticatedHandler
from common.access import scoped, AccessToken

from tornado.gen import coroutine, Return
from tornado.web import HTTPError


from model.event import EventNotFound, EventError


class EventJoinHandler(AuthenticatedHandler):
    @scoped(scopes=["event_join"])
    @coroutine
    def post(self, event_id):
        token = self.token
        account_id = token.account
        gamespace_id = token.get(AccessToken.GAMESPACE)

        score = self.get_argument("score", 0.0)
        leaderboard_info = self.get_argument("leaderboard_info", None)
        if leaderboard_info:
            try:
                leaderboard_info = json.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "JSON 'leaderboard_info' is corrupted")

        try:
            yield self.application.events.join_event(
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
    @coroutine
    def post(self, event_id):
        token = self.token
        account_id = token.account
        gamespace_id = token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        score = self.get_argument("score", 0.0)
        leaderboard_info = self.get_argument("leaderboard_info", None)
        if leaderboard_info:
            try:
                leaderboard_info = json.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "JSON 'leaderboard_info' is corrupted")

        try:
            yield self.application.events.join_group_event(
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
    @coroutine
    def post(self, event_id):
        account_id = self.current_user.token.account
        gamespace_id = self.current_user.token.get(AccessToken.GAMESPACE)

        try:
            yield self.application.events.leave_event(
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
    @coroutine
    def post(self, event_id):
        account_id = self.current_user.token.account
        gamespace_id = self.current_user.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        try:
            yield self.application.events.leave_event(
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
    @coroutine
    def get(self, event_id):
        events = self.application.events

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        path = self.get_argument("path", None)
        if path:
            path = filter(bool, path.split("/"))

        try:
            data = yield events.get_profile(
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
    @coroutine
    def post(self, event_id):
        events = self.application.events

        merge = self.get_argument("merge", "true") == "true"
        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        path = self.get_argument("path", None)
        if path:
            path = filter(bool, path.split("/"))

        try:
            profile = json.loads(self.get_argument("profile"))
        except:
            raise HTTPError(400, "Not a valid profile.")

        try:
            new_data = yield events.update_profile(
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
    @coroutine
    def get(self, event_id):
        events = self.application.events

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        path = self.get_argument("path", None)
        if path:
            path = filter(bool, path.split("/"))

        try:
            data = yield events.get_group_profile(
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
    @coroutine
    def post(self, event_id):
        events = self.application.events

        merge = self.get_argument("merge", "true") == "true"
        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        path = self.get_argument("path", None)
        if path:
            path = filter(bool, path.split("/"))

        try:
            group_profile = json.loads(self.get_argument("group_profile"))
        except:
            raise HTTPError(400, "Not a valid 'group_profile'.")

        try:
            new_data = yield events.update_group_profile(
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
    @coroutine
    def get(self, event_id):
        events = self.application.events

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        try:
            participants = yield events.list_group_account_participants(
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
                for account_id, participant in participants.iteritems()
            }
        })

    @scoped(scopes=["event_profile_write"])
    @coroutine
    def post(self, event_id):
        events = self.application.events

        merge = self.get_argument("merge", "true") == "true"
        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)
        group_id = self.get_argument("group_id")

        try:
            group_profile = json.loads(self.get_argument("group_profile"))
        except:
            raise HTTPError(400, "Not a valid 'group_profile'.")

        try:
            new_data = yield events.update_group_profile(
                gamespace_id, event_id, group_id,
                group_profile, merge)
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
    @coroutine
    def post(self, event_id):

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        auto_join = self.get_argument("auto_join", "true") == "true"
        score = self.get_argument("score")

        if auto_join and not self.has_scopes(["event_join"]):
            raise HTTPError(403, "Scope 'event_join' is required for auto_join")

        leaderboard_info = self.get_argument("leaderboard_info", None)
        if leaderboard_info:
            try:
                leaderboard_info = json.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "JSON 'leaderboard_info' is corrupted")

        try:
            new_score = yield self.application.events.add_score(
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
    @coroutine
    def post(self, event_id):

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
                leaderboard_info = json.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "JSON 'leaderboard_info' is corrupted")

        try:
            new_score = yield self.application.events.add_group_score(
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
    @coroutine
    def post(self, event_id):
        score = self.get_argument("score")

        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        auto_join = self.get_argument("auto_join", "true") == "true"

        if auto_join and not self.has_scopes(["event_join"]):
            raise HTTPError(403, "Scope 'event_join' is required for auto_join")

        leaderboard_info = self.get_argument("leaderboard_info")
        if leaderboard_info:
            try:
                leaderboard_info = json.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "JSON 'leaderboard_info' is corrupted")

        try:
            new_score = yield self.application.events.update_score(
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
    @coroutine
    def post(self, event_id):
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
                leaderboard_info = json.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "JSON 'leaderboard_info' is corrupted")

        try:
            new_score = yield self.application.events.update_group_score(
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
    @coroutine
    def get(self):

        events = self.application.events
        account_id = self.token.account
        gamespace_id = self.token.get(AccessToken.GAMESPACE)

        group_id = self.get_argument("group_id", 0)

        extra_start_time = self.get_argument("extra_start_time", 0)
        extra_end_time = self.get_argument("extra_end_time", self.get_argument("extra_time", 0))

        try:
            events_list = yield events.list_events(
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
