"""
Django settings — values sourced from config/config.yml via config.loader.
"""
import sys
from pathlib import Path

# Ensure project root is on sys.path so config.loader is importable
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from config.loader import get  # noqa: E402  (import after path manipulation)

SECRET_KEY = "django-insecure-ecommerce-etl-pipeline-secret-change-in-production"
DEBUG       = True
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "pipeline",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF     = "etl_core.urls"
WSGI_APPLICATION = "etl_core.wsgi.application"

DATABASES = {
    "default": {
        "ENGINE":   "django.db.backends.postgresql",
        "NAME":     get("database.name",     "etl_db"),
        "USER":     get("database.user",     "etl_user"),
        "PASSWORD": get("database.password", "etl_password"),
        "HOST":     get("database.host",     "localhost"),
        "PORT":     str(get("database.port", 5432)),
    }
}

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
STATIC_URL         = "/static/"
LANGUAGE_CODE      = "en-us"
TIME_ZONE          = "UTC"
USE_I18N           = True
USE_TZ             = True
