
from anthill.common.options import options
from anthill.common import server, access, keyvalue, database

from . import handler as h
from . import admin
from . import options as _opts
from . model.event import EventsModel


class EventsServer(server.Server):
    # noinspection PyShadowingNames
    def __init__(self):
        super(EventsServer, self).__init__()

        self.db = database.Database(
            host=options.db_host,
            database=options.db_name,
            user=options.db_username,
            password=options.db_password)

        self.events = EventsModel(self.db, self)

        self.cache = keyvalue.KeyValueStorage(
            host=options.cache_host,
            port=options.cache_port,
            db=options.cache_db,
            max_connections=options.cache_max_connections)

    def get_admin(self):
        return {
            "index": admin.RootAdminController,
            "events": admin.EventsController,
            "event": admin.EventController,
            "new_event": admin.NewEventController,
            "choose_category": admin.ChooseCategoryController,
            "categories": admin.CategoriesController,
            "category": admin.CategoryController,
            "new_category": admin.NewCategoryController,
            "common": admin.CommonController
        }

    def get_models(self):
        return [self.events]

    def get_internal_handler(self):
        return h.InternalHandler(self)

    def get_metadata(self):
        return {
            "title": "Events",
            "description": "Compete the players with time-limited events",
            "icon": "calendar"
        }

    def get_handlers(self):
        return [
            (r"/events", h.EventsHandler),
            (r"/event/(.*)/group/leave", h.EventGroupLeaveHandler),
            (r"/event/(.*)/group/join", h.EventGroupJoinHandler),
            (r"/event/(.*)/group/score/add", h.EventGroupAddScoreHandler),
            (r"/event/(.*)/group/score/update", h.EventGroupUpdateScoreHandler),
            (r"/event/(.*)/group/profile", h.EventGroupProfileHandler),
            (r"/event/(.*)/group/participants", h.EventGroupParticipantsHandler),
            (r"/event/(.*)/leave", h.EventLeaveHandler),
            (r"/event/(.*)/join", h.EventJoinHandler),
            (r"/event/(.*)/score/add", h.EventAddScoreHandler),
            (r"/event/(.*)/score/update", h.EventUpdateScoreHandler),
            (r"/event/(.*)/profile", h.EventProfileHandler),
        ]


if __name__ == "__main__":
    stt = server.init()
    access.AccessToken.init([access.public()])
    server.start(EventsServer)
