import common.admin as a

from tornado.gen import coroutine, Return
import logging
import ujson
import collections
import common

from model.event import EventNotFound, CategoryNotFound


class CategoriesController(a.AdminController):
    @coroutine
    def get(self):
        categories = yield self.application.events.list_categories(self.gamespace)

        result = {
            "categories": categories
        }

        raise Return(result)

    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("events", "Events")
            ], "Categories"),
            a.links("Categories", [
                a.link("category", category.name, "list-alt", category_id=category.category_id)
                for category in data["categories"]
            ]),
            a.links("Navigate", [
                a.link("events", "Go back", icon="chevron-left"),
                a.link("common", "Edit common template", icon="flask"),
                a.link("new_category", "Create a new category", icon="plus"),
                a.link("https://spacetelescope.github.io/understanding-json-schema/index.html", "See docs", icon="book")
            ])
        ]

    def access_scopes(self):
        return ["event_admin"]


class CategoryController(a.AdminController):
    @coroutine
    def delete(self, danger, **ingored):

        if danger != "confirm":
            raise a.Redirect("category", category_id=self.context.get("category_id"))

        category_id = self.context.get("category_id")
        yield self.application.events.delete_category(self.gamespace, category_id)

        raise a.Redirect("categories", message="Category has been deleted")

    @coroutine
    def get(self, category_id):
        category = yield self.application.events.get_category(self.gamespace, category_id)

        scheme_json = category.scheme

        result = {
            "scheme": scheme_json,
            "category_name": category.name
        }

        raise Return(result)

    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("events", "Events"),
                a.link("categories", "Categories")
            ], data["category_name"]),
            a.form("Category template", fields={
                "scheme": a.field("scheme", "json", "primary"),
                "category_name": a.field("Category name (ID)", "text", "primary", "non-empty")
            }, methods={
                "update": a.method("Update", "primary"),
            }, data=data),
            a.split([
                a.notice(
                    "About templates",
                    "Each category template has a common template shared across categories. "
                    "Category template inherits a common template."
                ),
                a.form("Danger", fields={
                    "danger": a.field("This cannot be undone! The events of this category will be also deleted! "
                                      "Type 'confirm' to do this.", "text", "danger",
                                      "non-empty")
                }, methods={
                    "delete": a.method("Delete category", "danger"),
                }, data=data),
            ]),
            a.links("Navigate", [
                a.link("events", "Go back", icon="chevron-left"),
                a.link("common", "Edit common template", icon="flask"),
                a.link("events", "See events of this category", category=self.context.get("category_id")),
                a.link("https://spacetelescope.github.io/understanding-json-schema/index.html", "See docs", icon="book")
            ])
        ]

    def access_scopes(self):
        return ["event_admin"]

    @coroutine
    def update(self, scheme, category_name):

        category_id = self.context.get("category_id")

        try:
            scheme_data = ujson.loads(scheme)
        except (KeyError, ValueError):
            raise a.ActionError("Corrupted json")

        yield self.application.events.update_category(self.gamespace, category_id, scheme_data, category_name)

        raise a.Redirect(
            "category",
            message="Category has been updated",
            category_id=category_id)


class ChooseCategoryController(a.AdminController):
    @coroutine
    def apply(self, category):
        raise a.Redirect("new_event", category=category)

    @coroutine
    def get(self, category=None):

        categories = yield self.application.events.list_categories(self.gamespace)

        raise Return({
            "category": category,
            "categories": {
                cat.category_id: cat.name for cat in categories
            }
        })

    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("events", "Events")
            ], "Choose category"),
            a.form(
                title="Choose event category to create event of",
                fields={
                    "category": a.field(
                        "Select category", "select", "primary", values=data["categories"]
                    )
                }, methods={
                    "apply": a.method("Proceed", "primary")
                }, data=data
            ),
            a.links("Navigation", links=[
                a.link("events", "Go back", icon="chevron-left"),
                a.link("categories", "Manage categories", "list-alt")
            ])
        ]

    def access_scopes(self):
        return ["event_admin"]


class CommonController(a.AdminController):
    @coroutine
    def get(self):
        scheme = yield self.application.events.get_common_scheme(self.gamespace)

        result = {
            "scheme": scheme
        }

        raise Return(result)

    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("events", "Events"),
                a.link("categories", "Categories")
            ], "Common template"),
            a.form("Common template", fields={
                "scheme": a.field("scheme", "json", "primary")
            }, methods={
                "update": a.method("Update", "primary"),
            }, data=data),
            a.links("Navigate", [
                a.link("@back", "Go back", icon="chevron-left"),
                a.link("https://spacetelescope.github.io/understanding-json-schema/index.html", "See docs", icon="book")
            ])
        ]

    def access_scopes(self):
        return ["event_admin"]

    @coroutine
    def update(self, scheme):
        try:
            scheme_data = ujson.loads(scheme)
        except (KeyError, ValueError):
            raise a.ActionError("Corrupted json")

        yield self.application.events.update_common_scheme(self.gamespace, scheme_data)
        raise a.Redirect("common", message="Common template has been updated")


class EventController(a.AdminController):
    @coroutine
    def delete(self, **ignored):

        event_id = self.context.get("event_id")

        try:
            event = yield self.application.events.get_event(self.gamespace, event_id)
        except EventNotFound:
            raise a.ActionError("No such event")

        yield self.application.events.delete_event(self.gamespace, event_id)
        raise a.Redirect(
            "events",
            message="Event has been deleted",
            category=event.category_id)

    @coroutine
    def get(self, event_id):

        events = self.application.events

        try:
            event = yield events.get_event(self.gamespace, event_id)
        except EventNotFound as e:
            raise a.ActionError("Event '" + str(event_id) + "' was not found.")

        category_id = event.category_id
        category_name = event.category

        enabled = "true" if event.enabled else "false"
        tournament = "true" if event.tournament else "false"
        clustered = "true" if event.clustered else "false"
        start_dt = str(event.time_start)
        end_dt = str(event.time_end)

        common_scheme = yield events.get_common_scheme(self.gamespace)
        category = yield events.get_category(self.gamespace, category_id)
        category_scheme = category.scheme

        scheme = common_scheme.copy()
        common.update(scheme, category_scheme)

        raise Return({
            "enabled": enabled,
            "tournament": tournament,
            "clustered": clustered,
            "event": event,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "event_data": event.data,
            "scheme": scheme,
            "category": category_id,
            "category_name": category_name
        })

    def render(self, data):

        category = data["category"]

        return [
            a.breadcrumbs([
                a.link("events", "Events", category=category),
            ], "Event"),
            a.form(
                title="Event editor",
                fields={
                    "event_data": a.field(
                        "Event properties", "dorn", "primary",
                        schema=data["scheme"], order=6
                    ),
                    "enabled": a.field("Is event enabled", "switch", "primary", order=3),
                    "tournament": a.field("Is tournament enabled", "switch", "primary", order=4),
                    "clustered": a.field("Is tournament's leaderboard clustered", "switch", "primary",
                                         readonly=True, order=5),
                    "category_name": a.field("Category", "readonly", "primary"),
                    "start_dt": a.field("Start date", "date", "primary", order=1),
                    "end_dt": a.field("End date", "date", "primary", order=2)
                },
                methods={
                    "save": a.method("Save", "primary"),
                    "delete": a.method("Delete event", "danger")
                },
                data=data
            ),
            a.links("Navigate", [
                a.link("events", "Go back", icon="chevron-left", category=category),
                a.link("category", "Edit category", category_id=category),
                a.link("new_event", "Clone event", icon="clone",
                       clone=self.context.get("event_id"),
                       category=data.get("category"))
            ])
        ]

    @coroutine
    def save(self, event_data, start_dt, end_dt, enabled="false", tournament="false", **ignore):
        event_id = self.context.get("event_id")

        logging.debug("Saving event '%s'", event_id)
        yield self.application.events.update_event(
            self.gamespace, event_id, enabled, tournament, ujson.loads(event_data), start_dt, end_dt
        )

        raise a.Redirect(
            "event",
            message="Event has been updated",
            event_id=event_id)

    def access_scopes(self):
        return ["event_admin"]


class EventsController(a.AdminController):
    EVENTS_IN_PAGE = 20

    @coroutine
    def apply(self, category=None):

        if not category:
            raise a.Redirect("choose_category")

        raise a.Redirect("events", category=category)

    @coroutine
    def get(self, category=None, page=1):
        categories = yield self.application.events.list_categories(
            self.gamespace)

        events, pages = yield self.application.events.list_paged_events(
            self.gamespace,
            category,
            EventsController.EVENTS_IN_PAGE, page)

        cats = {
            cat.category_id: cat.name
            for cat in categories
        }

        cats[0] = "< Select >"

        raise Return({
            "events": events,
            "category": category,
            "categories": cats,
            "pages": pages
        })

    def render(self, data):
        tbl_rows = []

        for event in data["events"]:

            title = "unknown"
            description = "unknown"

            if "title" in event.data:
                title = event.data["title"].get("EN", "unknown")

            if "description" in event.data:
                description = event.data["description"].get("EN", "unknown")

            tbl_tr = {
                "edit": [a.link("event", event.item_id, icon="calendar", event_id=event.item_id)],
                "enabled": "yes" if event.enabled else "no",
                "tournament": "yes" + (" (clustered)" if event.clustered else "") if event.tournament else "no",
                "name": title,
                "description": description,
                "category": event.category,
                "dates": str(event.time_start) + " -<br> " + str(event.time_end),
                "controls": [a.button("event", "Delete", "danger", _method="delete", event_id=event.item_id)]
            }

            tbl_rows.append(tbl_tr)

        return [
            a.breadcrumbs([], "Events"),
            a.form(
                title="Filters",
                fields={
                    "category": a.field(
                        "Category", "select", "primary", values=data["categories"]
                    )
                }, methods={
                    "apply": a.method("Apply", "primary")
                }, data=data
            ),
            a.content("Events", [
                {
                    "id": "edit",
                    "title": "Edit"
                }, {
                    "id": "name",
                    "title": "Name"
                }, {
                    "id": "description",
                    "title": "Description"
                }, {
                    "id": "enabled",
                    "title": "Enabled"
                }, {
                    "id": "tournament",
                    "title": "Tournament"
                }, {
                    "id": "category",
                    "title": "Category"
                }, {
                    "id": "dates",
                    "title": "Dates"
                }, {
                    "id": "controls",
                    "title": "Controls"
                }], tbl_rows, "default"),
            a.pages(data["pages"]),
            a.links("Navigation", links=[
                a.link("choose_category", "Create new event", "plus", category=self.context.get("category", "0")),
                a.link("categories", "Manage categories", "list-alt")
            ])
        ]

    def access_scopes(self):
        return ["event_admin"]


class NewCategoryController(a.AdminController):
    @coroutine
    def create(self, scheme, category_name):

        try:
            scheme_data = ujson.loads(scheme)
        except (KeyError, ValueError):
            raise a.ActionError("Corrupted json")

        category_id = yield self.application.events.create_category(self.gamespace, category_name, scheme_data)

        raise a.Redirect(
            "category",
            message="Category has been created",
            category_id=category_id)

    def render(self, data):
        return [
            a.breadcrumbs([
                a.link("events", "Events"),
                a.link("categories", "Categories")
            ], "New category"),
            a.form("Category template", fields={
                "scheme": a.field("scheme", "json", "primary"),
                "category_name": a.field("Category name (ID)", "text", "primary", "non-empty")
            }, methods={
                "create": a.method("Create", "primary"),
            }, data={"scheme": {}}),
            a.notice(
                "About templates",
                "Each category template has a common template shared across categories. "
                "Category template inherits a common template."
            ),
            a.links("Navigate", [
                a.link("categories", "Go back", icon="chevron-left"),
                a.link("common", "Edit common template", icon="flask"),
                a.link("events", "See events of this category", category=self.context.get("category_id")),
                a.link("https://spacetelescope.github.io/understanding-json-schema/index.html", "See docs", icon="book")
            ])
        ]

    def access_scopes(self):
        return ["event_admin"]


class NewEventController(a.AdminController):
    @coroutine
    def create(self, event_data, start_dt, end_dt, enabled="false", tournament="false", clustered="false", **ignore):
        category_id = self.context.get("category")

        try:
            event_id = yield self.application.events.create_event(
                self.gamespace, category_id, enabled, tournament, clustered, ujson.loads(event_data), start_dt, end_dt
            )
        except CategoryNotFound:
            raise a.ActionError("Category not found")

        raise a.Redirect(
            "event",
            message="Event has been created",
            event_id=event_id)

    @coroutine
    def get(self, category, clone=None):

        events = self.application.events

        common_scheme = yield events.get_common_scheme(self.gamespace)

        category = yield events.get_category(self.gamespace, category)

        category_name = category.name
        category_scheme = category.scheme

        def update(d, u):
            for k, v in u.iteritems():
                if isinstance(v, collections.Mapping):
                    r = update(d.get(k, {}), v)
                    d[k] = r
                else:
                    d[k] = u[k]
            return d

        scheme = common_scheme.copy()
        update(scheme, category_scheme)

        event_data = None
        start_dt = None
        end_dt = None
        enabled = "true"
        tournament = "false"

        if clone:

            try:
                event = yield events.get_event(self.gamespace, clone)
            except EventNotFound as e:
                raise a.ActionError("Event '" + str(clone) + "' was not found.")

            event_data = event.data
            enabled = "true" if event.enabled else "false"
            tournament = "true" if event.tournament else "false"
            start_dt = str(event.time_start)
            end_dt = str(event.time_end)

        raise Return({
            "scheme": scheme,
            "enabled": enabled,
            "tournament": tournament,
            "category_name": category_name,
            "event_data": event_data,
            "start_dt": start_dt,
            "end_dt": end_dt
        })

    def render(self, data):

        category = self.context.get("category")

        return [
            a.breadcrumbs([
                a.link("events", "Events", category=category),
            ], "New event"),
            a.form(
                title="New event (of category " + data.get("category_name") + ")",
                fields={
                    "event_data": a.field(
                        "Event properties", "dorn", "primary",
                        schema=data["scheme"], order=5
                    ),
                    "enabled": a.field("Is event enabled", "switch", "primary", order=3),
                    "tournament": a.field("Is tournament enabled", "switch", "primary", order=4),
                    "clustered": a.field("Is tournament's leaderboard clustered (cannot be changed later)",
                                         "switch", "primary", order=4),

                    "start_dt": a.field("Start date", "date", "primary", "non-empty", order=1),
                    "end_dt": a.field("End date", "date", "primary", "non-empty", order=2)
                },
                methods={
                    "create": a.method("Create", "primary")
                },
                data=data
            ),
            a.links("Navigate", [
                a.link("events", "Go back", icon="chevron-left", category=category),
                a.link("category", "Edit category", category_id=category)
            ])
        ]

    def access_scopes(self):
        return ["event_admin"]


class RootAdminController(a.AdminController):
    def render(self, data):
        return [
            a.links("Events service", [
                a.link("events", "Edit events", icon="wrench")
            ])
        ]

    def access_scopes(self):
        return ["event_admin"]
