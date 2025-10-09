from .auth import auth_bp
from .applications import applications_bp
from .users import users_bp
from .workshops import workshops_bp
from .vehicles import vehicles_bp
from .stickers import stickers_bp
from .inspections import inspections_bp
from .certificates import certificates_bp
from .application_documents import docs_bp
from .persons import persons_bp
from .inspection_documents import inspection_docs_bp
from .payments import payments_bp
from .payment_receipts import payment_receipts_bp
from .payments_admin import payments_admin_bp
from .statistics import statistics_bp

def register_routes(app):
    app.register_blueprint(auth_bp, url_prefix="/auth")
    app.register_blueprint(applications_bp, url_prefix="/applications")
    app.register_blueprint(users_bp, url_prefix="/users")
    app.register_blueprint(workshops_bp, url_prefix="/workshops")
    app.register_blueprint(stickers_bp, url_prefix="/stickers")
    app.register_blueprint(vehicles_bp, url_prefix="/vehicles")
    app.register_blueprint(inspections_bp, url_prefix="/inspections")
    app.register_blueprint(certificates_bp, url_prefix="/certificates")
    app.register_blueprint(docs_bp, url_prefix="/docs")
    app.register_blueprint(persons_bp, url_prefix="/persons")
    app.register_blueprint(inspection_docs_bp, url_prefix="/inspections")
    app.register_blueprint(payments_bp, url_prefix="/payments")
    app.register_blueprint(payment_receipts_bp, url_prefix="/payment_receipts")
    app.register_blueprint(payments_admin_bp , url_prefix="/payments_admin")
    app.register_blueprint(statistics_bp , url_prefix="/statistics")
