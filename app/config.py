import os
import logging
from dotenv import load_dotenv

# Try to load .env variables if present
try:
    load_dotenv()
except Exception:
    pass

# Initialize Structured Enterprise Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("erp_server")

# Environment Variables
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.critical("DATABASE_URL environment variable is not set! Please add DATABASE_URL to your .env file.")

JWT_SECRET = os.environ.get("JWT_SECRET")
if not JWT_SECRET:
    JWT_SECRET = "super-secret-key-change-in-prod"
    if os.environ.get("RENDER") or os.environ.get("ENV") == "production":
        logger.warning("JWT_SECRET environment variable is not set! Using default key.")

PORT = int(os.environ.get("PORT", 10000))
WORKERS = int(os.environ.get("WORKERS", 1))

# Enterprise Performance In-Memory Caches Configurations
CACHE_TTL = 60  # Check authorization freshness every 60 seconds
LOOKUP_CACHE_TTL = 300  # 5 minutes

SCHEMA_MUTATING_TABLES = {
    'phc_screens_t', 'phc_module_t', 'phc_roles_t', 
    'phc_role_screen_assignment_t', 'phc_user_roles_assignment_t', 
    'phc_users_t', 'phc_lookup_values_t', 'phc_lookup_types', 'phc_lookup_types_t'
}

# Icon Maps
MODULE_ICON_MAP = {
    'general': 'layers',
    'erpadmin': 'settings-2',
    'erp admin': 'settings-2',
    'admin': 'settings-2',
    'masterdata': 'database',
    'master data': 'database',
    'cleaning': 'sparkles',
    'cleaning validation': 'sparkles',
    'quality': 'shield-check',
    'qa': 'shield-check',
    'qc': 'test-tube-2',
    'facilities': 'building-2',
    'facility': 'building-2',
    'plant': 'factory',
    'ehs': 'activity',
    'ehs & safety': 'activity',
    'safety': 'activity',
    'hr': 'users',
    'human resources': 'users',
    'inventory': 'box',
    'materials': 'package',
    'finance': 'wallet',
    'gl': 'book-open',
    'ap': 'receipt',
    'ar': 'credit-card',
    'procurement': 'shopping-cart',
    'purchasing': 'shopping-bag',
    'sales': 'trending-up',
    'manufacturing': 'cpu',
    'production': 'factory',
    'maintenance': 'wrench',
    'lab': 'flask-conical',
    'laboratory': 'flask-conical',
    'documents': 'file-text',
    'security': 'lock',
    'compliance': 'clipboard-check',
    'supply chain': 'truck',
    'logistics': 'truck',
    'calibration': 'scale',
    'workflow': 'workflow',
    'reports': 'bar-chart-3',
}

CURATED_ICON_LIST = [
    "layers", "database", "shield-check", "settings-2", "sparkles",
    "flask-conical", "clipboard-list", "building-2", "users", "cpu",
    "box", "package", "truck", "file-text", "activity", "heart-pulse",
    "scale", "workflow", "archive", "lock", "gauge", "test-tube-2",
    "wallet", "shopping-cart", "factory", "wrench", "bar-chart-3",
    "briefcase", "compass", "book-open", "grid", "folder", "globe", "zap"
]

# Security / Rate Limiting
RATE_LIMIT_WINDOW = 900  # 15 minutes
MAX_LOGIN_ATTEMPTS = 5   # max failed attempts per window

# File Upload Settings
ALLOWED_EXTENSIONS = {
    '.pdf', '.png', '.jpg', '.jpeg', '.webp', '.svg', '.gif', 
    '.csv', '.xlsx', '.xls', '.doc', '.docx', '.txt', '.json'
}
BLOCKED_EXTENSIONS = {
    '.py', '.sh', '.bat', '.cmd', '.exe', '.dll', '.php', '.phtml', 
    '.js', '.vbs', '.ps1', '.jsp', '.cgi', '.jar', '.com', '.scr', '.msi'
}
MAX_UPLOAD_SIZE = 15 * 1024 * 1024  # 15 MB
