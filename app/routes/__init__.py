from .auth import auth_bp
from .applications import applications_bp
from .users import users_bp
from .workshops import workshops_bp

def register_routes(app):
    app.register_blueprint(auth_bp, url_prefix="/auth")
    app.register_blueprint(applications_bp, url_prefix="/applications")
    app.register_blueprint(users_bp, url_prefix="/users")
    app.register_blueprint(workshops_bp, url_prefix="/workshops")
