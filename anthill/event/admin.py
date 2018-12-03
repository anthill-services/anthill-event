
from anthill.common.validate import validate
from anthill.common import admin as a, update

from . model.event import EventNotFound, CategoryNotFound, EventFlags, EventEndAction

import ujson
import collections


EVENT_END_ACTION_DESCRIPTION = """
<b>Send Message</b><br>A message with detailed information about event (including score, rank, profile) 
will be sent to the participating players<br><br>
<b>Call Exec Function</b><br>A function on exec service will be called with detailed information about event (including 
score, rank, profile). In that case the Server Code should be enabled, with function with name <code>event_completed</code>:
<pre><code>async function event_completed(args)
{
&nbsp;&nbsp;&nbsp;&nbsp;// args[\"event\"] would contain event info
&nbsp;&nbsp;&nbsp;&nbsp;// args[\"participants\"] would contain a list of participation objects to process 
&nbsp;&nbsp;&nbsp;&nbsp;// (one object for each player/participant), like so:
&nbsp;&nbsp;&nbsp;&nbsp;{
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\"account\": &lt;account id&gt;, // or \"group\" for group-based event
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\"profile\": &lt;participation profile&gt;, 
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\"score\": &lt;score&gt;, 
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;\"rank\": &lt;rank&gt;
&nbsp;&nbsp;&nbsp;&nbsp;}
}
event_completed.allow_call = true;
</code></pre><br>
"""


class CategoriesController(a.AdminController):
    async def get(self):
        categories = await self.application.events.list_categories(self.gamespace)

        result = {
            "categories": categories
        }

        return result

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
    async def delete(self, danger, **ingored):

        if danger != "confirm":
            raise a.Redirect("category", category_id=self.context.get("category_id"))

        category_id = self.context.get("category_id")
        await self.application.events.delete_category(self.gamespace, category_id)

        raise a.Redirect("categories", message="Category has been deleted")

    async def get(self, category_id):
        category = await self.application.events.get_category(self.gamespace, category_id)

        scheme_json = category.scheme

        result = {
            "scheme": scheme_json,
            "category_name": category.name
        }

        return result

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

    async def update(self, scheme, category_name):

        category_id = self.context.get("category_id")

        try:
            scheme_data = ujson.loads(scheme)
        except (KeyError, ValueError):
            raise a.ActionError("Corrupted json")

        await self.application.events.update_category(self.gamespace, category_id, scheme_data, category_name)

        raise a.Redirect(
            "category",
            message="Category has been updated",
            category_id=category_id)


class ChooseCategoryController(a.AdminController):
    async def apply(self, category):
        raise a.Redirect("new_event", category=category)

    async def get(self, category=None):

        categories = await self.application.events.list_categories(self.gamespace)

        return {
            "category": category,
            "categories": {
                cat.category_id: cat.name for cat in categories
            }
        }

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
    async def get(self):
        scheme = await self.application.events.get_common_scheme(self.gamespace)

        result = {
            "scheme": scheme
        }

        return result

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

    async def update(self, scheme):
        try:
            scheme_data = ujson.loads(scheme)
        except (KeyError, ValueError):
            raise a.ActionError("Corrupted json")

        await self.application.events.update_common_scheme(self.gamespace, scheme_data)
        raise a.Redirect("common", message="Common template has been updated")


class EventController(a.AdminController):
    async def delete(self, **ignored):

        event_id = self.context.get("event_id")

        try:
            event = await self.application.events.get_event(self.gamespace, event_id)
        except EventNotFound:
            raise a.ActionError("No such event")

        await self.application.events.delete_event(self.gamespace, event_id)
        raise a.Redirect(
            "events",
            message="Event has been deleted",
            category=event.category_id)

    async def get(self, event_id):

        events = self.application.events

        try:
            event = await events.get_event(self.gamespace, event_id)
        except EventNotFound:
            raise a.ActionError("Event was not found.")

        category_id = event.category_id
        category_name = event.category

        enabled = "true" if event.enabled else "false"
        tournament = "true" if event.tournament else "false"
        clustered = "true" if event.clustered else "false"
        group = "true" if event.group else "false"
        start_dt = str(event.time_start)
        end_dt = str(event.time_end)
        end_action = str(event.end_action)

        common_scheme = await events.get_common_scheme(self.gamespace)
        category = await events.get_category(self.gamespace, category_id)
        category_scheme = category.scheme

        scheme = common_scheme.copy()
        update(scheme, category_scheme)

        return {
            "enabled": enabled,
            "tournament": tournament,
            "clustered": clustered,
            "group": group,
            "event": event,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "event_data": event.data,
            "scheme": scheme,
            "category": category_id,
            "category_name": category_name,
            "end_action": end_action
        }

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
                        schema=data["scheme"], order=8
                    ),
                    "enabled": a.field("Is event enabled", "switch", "primary", order=3),
                    "tournament": a.field("Is tournament enabled (e.g. players will be ranked)",
                                          "switch", "primary", order=4),
                    "clustered": a.field("Is tournament's leaderboard clustered", "switch", "primary",
                                         readonly=True, order=5),
                    "group": a.field("Is event group-based", "switch", "primary",
                                         readonly=True, order=6),

                    "end_action": a.field("Action Once Event Is Complete", "select", "primary", order=7, values={
                        EventEndAction.NONE: "Do nothing",
                        EventEndAction.MESSAGE: "Send Message",
                        EventEndAction.EXEC: "Call Exec Function"
                    }, description=EVENT_END_ACTION_DESCRIPTION),

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
                a.link("category", "Edit category", icon="list-alt", category_id=category),
                a.link("new_event", "Clone event", icon="clone",
                       clone=self.context.get("event_id"),
                       category=data.get("category"))
            ])
        ]

    @validate(event_data="load_json_dict", start_dt="datetime", end_dt="datetime",
              enabled="bool", tournament="bool", end_action="str")
    async def save(self, event_data, start_dt, end_dt, enabled=False, tournament=False,
             end_action=EventEndAction.NONE, **ignore):
        event_id = self.context.get("event_id")

        events = self.application.events

        try:
            event = await events.get_event(self.gamespace, event_id)
        except EventNotFound:
            raise a.ActionError("Event was not found.")

        flags = event.flags

        flags.set(EventFlags.TOURNAMENT, tournament)

        end_action = EventEndAction(end_action)

        await events.update_event(
            self.gamespace, event_id, enabled, flags,
            event_data, start_dt, end_dt, end_action)

        raise a.Redirect(
            "event",
            message="Event has been updated",
            event_id=event_id)

    def access_scopes(self):
        return ["event_admin"]


class EventsController(a.AdminController):
    EVENTS_IN_PAGE = 20

    async def apply(self, category=None):

        if not category:
            raise a.Redirect("choose_category")

        raise a.Redirect("events", category=category)

    @validate(category="int", page="int")
    async def get(self, category=0, page=1):
        categories = await self.application.events.list_categories(
            self.gamespace)

        events, pages = await self.application.events.list_paged_events(
            self.gamespace,
            EventsController.EVENTS_IN_PAGE, page,
            category_id=category)

        cats = {
            cat.category_id: cat.name
            for cat in categories
        }

        cats[0] = "< Select >"

        return {
            "events": events,
            "category": category,
            "categories": cats,
            "pages": pages
        }

    def render(self, data):
        tbl_rows = []

        for event in data["events"]:

            title = "unknown"
            description = "unknown"

            if "title" in event.data:
                title_object = event.data["title"]
                title = title_object.get("EN", title_object.get("en", "unknown"))
            elif "name" in event.data:
                title_object = event.data["name"]
                title = title_object.get("EN", title_object.get("en", "unknown"))

            if "description" in event.data:
                description_object = event.data["description"]
                description = description_object.get("EN", description_object.get("en", "unknown"))

            tbl_tr = {
                "edit": [a.link("event", event.item_id, icon="calendar", event_id=event.item_id)],
                "enabled": "yes" if event.enabled else "no",
                "tournament": "yes" + (" (clustered)" if event.clustered else "") if event.tournament else "no",
                "name": title[:32],
                "description": description[:32],
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
    async def create(self, scheme, category_name):

        try:
            scheme_data = ujson.loads(scheme)
        except (KeyError, ValueError):
            raise a.ActionError("Corrupted json")

        category_id = await self.application.events.create_category(self.gamespace, category_name, scheme_data)

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
    @validate(event_data="load_json_dict", start_dt="datetime", end_dt="datetime", enabled="bool",
              tournament="bool", clustered="bool", group="bool", end_action="str_name")
    async def create(self, event_data, start_dt, end_dt,
                     enabled=False, tournament=False, clustered=False, group=False,
                     end_action=EventEndAction.NONE, **ignore):
        category_id = self.context.get("category")

        flags = EventFlags()

        if tournament:
            flags.set(EventFlags.TOURNAMENT)

        if clustered:
            flags.set(EventFlags.CLUSTERED)

        if group:
            flags.set(EventFlags.GROUP)

        end_action = EventEndAction(end_action)

        try:
            event_id = await self.application.events.create_event(
                self.gamespace, category_id, enabled, flags,
                event_data, start_dt, end_dt, end_action)
        except CategoryNotFound:
            raise a.ActionError("Category not found")

        raise a.Redirect(
            "event",
            message="Event has been created",
            event_id=event_id)

    @validate(category="int", clone="int")
    async def get(self, category, clone=None):

        events = self.application.events

        common_scheme = await events.get_common_scheme(self.gamespace)

        category = await events.get_category(self.gamespace, category)

        category_name = category.name
        category_scheme = category.scheme

        def update(d, u):
            for k, v in u.items():
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
        clustered = "false"
        group = "false"
        end_action = EventEndAction.NONE

        if clone:

            try:
                event = await events.get_event(self.gamespace, clone)
            except EventNotFound:
                raise a.ActionError("Event was not found.")

            event_data = event.data
            enabled = "true" if event.enabled else "false"
            tournament = "true" if event.tournament else "false"
            clustered = "true" if event.clustered else "false"
            group = "true" if event.group else "false"
            start_dt = str(event.time_start)
            end_dt = str(event.time_end)
            end_action = str(event.end_action)

        return {
            "scheme": scheme,
            "enabled": enabled,
            "tournament": tournament,
            "clustered": clustered,
            "group": group,
            "category_name": category_name,
            "event_data": event_data,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "end_action": end_action
        }

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
                        schema=data["scheme"], order=8
                    ),
                    "enabled": a.field("Is event enabled", "switch", "primary", order=3),
                    "tournament": a.field("Is tournament enabled (e.g. players will be ranked)",
                                          "switch", "primary", order=4),
                    "clustered": a.field("Is tournament's leaderboard clustered",
                                         "switch", "primary", order=5,
                                         description="Cannot be changed later"),
                    "group": a.field("In even group-based",
                                     "switch", "primary", order=6,
                                     description="Cannot be changed later"),

                    "end_action": a.field("Action Once Event Is Complete", "select", "primary", order=7, values={
                        EventEndAction.NONE: "Do nothing",
                        EventEndAction.MESSAGE: "Send Message",
                        EventEndAction.EXEC: "Call Exec Function"
                    }, description=EVENT_END_ACTION_DESCRIPTION),

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
                a.link("category", "Edit category", icon="list-alt", category_id=category)
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
