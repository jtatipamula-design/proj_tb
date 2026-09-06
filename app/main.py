import os
from sanic import Sanic
from app.config import PORT, WORKERS, logger

from app.database import setup_db, close_db
from app.auth import setup_request_context
from app.routes.auth import auth_bp
from app.routes.ui import ui_bp
from app.routes.api import api_bp
from app.routes.system import system_bp

def create_app() -> Sanic:
    app = Sanic("ERP_System")
    app.config.OAS = False
    
    # Static files if they exist
    if os.path.isdir("static"):
        app.static("/static", "./static")

    # DB Hooks
    app.before_server_start(setup_db)
    app.after_server_stop(close_db)

    # Middleware
    app.middleware("request")(setup_request_context)

    # Blueprints
    app.blueprint(auth_bp)
    app.blueprint(ui_bp)
    app.blueprint(api_bp)
    app.blueprint(system_bp)
    
    return app

app = create_app()
