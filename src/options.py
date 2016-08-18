
from common.options import define

# Main

define("host",
       default="http://event-dev.anthill.local",
       help="Public hostname of this service",
       type=str)

define("listen",
       default="port:10700",
       help="Socket to listen. Could be a port number (port:N), or a unix domain socket (unix:PATH)",
       type=str)

define("name",
       default="event",
       help="Service short name. User to discover by discovery service.",
       type=str)

# MySQL database

define("db_host",
       default="127.0.0.1",
       type=str,
       help="MySQL database location")

define("db_username",
       default="anthill",
       type=str,
       help="MySQL account username")

define("db_password",
       default="",
       type=str,
       help="MySQL account password")

define("db_name",
       default="event",
       type=str,
       help="MySQL database name")
