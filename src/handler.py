
import logging
import traceback
import json

from common.handler import AuthenticatedHandler
from common.access import scoped, AccessToken

from tornado.gen import coroutine, Return
from tornado.web import HTTPError


from model.event import EventNotFound, EventError


class EventJoinHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def post(self, event_id):

        token = self.token

        account_id = token.account
        gamespace_id = token.get(
            AccessToken.GAMESPACE)

        try:
            yield self.application.events.join_event(
                gamespace_id,
                event_id,
                account_id)

        except EventError as e:
            raise HTTPError(
                e.code,
                str(e))

        except EventNotFound:
            raise HTTPError(
                404,
                "Event '%s' was not found." % event_id)

        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500,
                "Failed to engage user '{0}' in the event '{1}': {2}".format(
                    account_id, event_id, e
                )
            )


class EventLeaveHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def post(self, event_id):
        account_id = self.current_user.token.account
        gamespace_id = self.current_user.token.get(
            AccessToken.GAMESPACE)

        try:
            yield self.application.events.leave_event(
                gamespace_id,
                event_id,
                account_id)

        except EventError as e:
            raise HTTPError(
                e.code,
                str(e))

        except EventNotFound:
            raise HTTPError(
                404,
                "Event '%s' was not found." % event_id)

        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500,
                "Failed to make user '{0}' to leave the event '{1}': {2}".format(
                    account_id, event_id, e
                )
            )


class EventProfileHandler(AuthenticatedHandler):
    @scoped(scopes=["event_profile_write"])
    @coroutine
    def post(self, event_id):

        token = self.token

        try:
            data = json.loads(self.get_argument("data"))
        except:
            raise HTTPError(400, "Not a valid data.")

        merge = self.get_argument("merge", "true") == "true"

        account_id = token.account
        gamespace_id = token.get(AccessToken.GAMESPACE)

        try:
            new_data = yield self.application.events.update_profile(
                gamespace_id,
                event_id,
                account_id,
                data,
                merge)

        except EventError as e:
            raise HTTPError(
                e.code,
                str(e))

        except EventNotFound:
            raise HTTPError(
                404,
                "Event '%s' was not found." % event_id)

        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500,
                "Failed to update profile for the user '{0}' in the event '{1}': {2}".format(
                    account_id, event_id, e))
        else:
            self.write(json.dumps(new_data))


class EventScoreHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def post(self, event_id):

        token = self.token

        try:
            score = float(self.get_argument("score"))
        except:
            raise HTTPError(400, "Not a valid score.")

        account_id = token.account
        gamespace_id = token.get(AccessToken.GAMESPACE)

        leaderboard_info = self.get_argument("leaderboard_info")
        if leaderboard_info:
            try:
                leaderboard_info = json.loads(leaderboard_info)
            except (KeyError, ValueError):
                raise HTTPError(400, "JSON 'leaderboard_info' is corrupted")

        try:
            new_score = yield self.application.events.add_score(
                gamespace_id,
                event_id,
                account_id,
                score,
                leaderboard_info)

        except EventError as e:
            raise HTTPError(
                e.code,
                str(e))

        except EventNotFound:
            raise HTTPError(
                404,
                "Event '%s' was not found." % event_id)

        except Exception as e:
            logging.error(traceback.format_exc())
            raise HTTPError(
                500,
                "Failed to update score for the user '{0}' in the event '{1}': {2}".format(
                    account_id, event_id, e))
        else:
            result = {
                "score": new_score
            }
            self.write(json.dumps(result))


class EventsHandler(AuthenticatedHandler):
    @scoped()
    @coroutine
    def get(self):

        account_id = self.current_user.token.account
        gamespace_id = self.current_user.token.get(AccessToken.GAMESPACE)

        try:
            events = yield self.application.events.get_events(gamespace_id, account_id)

            result = json.dumps({
                "events": [event.dump() for event in events]
            })

            self.write(result)
        except Exception as e:
            raise HTTPError(
                500, "Failed to fetch a list of "
                "current events available for user '{0}': {1}".format(account_id, e)
            )


