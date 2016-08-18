
from common.options import options

import handler
import common.server
import common.access
import common.sign
import common.keyvalue
import common.database

import admin
import options as _opts

from model.event import EventsModel


class EventsServer(common.server.Server):
    # noinspection PyShadowingNames
    def __init__(self):
        super(EventsServer, self).__init__()

        self.db = common.database.Database(
            host=options.db_host,
            database=options.db_name,
            user=options.db_username,
            password=options.db_password)

        self.events = EventsModel(self.db)

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

    def get_metadata(self):
        return {
            "title": "Events",
            "description": "Compete the players with time-limited events",
            "icon": "calendar"
        }

    def get_handlers(self):
        return [
            (r"/events", handler.EventsHandler),
            (r"/event/(.*)/leave", handler.EventLeaveHandler),
            (r"/event/(.*)/join", handler.EventJoinHandler),
            (r"/event/(.*)/score", handler.EventScoreHandler),
            (r"/event/(.*)/profile", handler.EventProfileHandler),
        ]

if __name__ == "__main__":
    stt = common.server.init()
    common.access.AccessToken.init([common.access.public()])
    common.server.start(EventsServer)
