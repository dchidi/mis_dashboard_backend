import os
from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

template_env = Environment(
    loader=FileSystemLoader(os.path.join(BASE_DIR, "email_templates")),
    autoescape=select_autoescape(["html", "xml"]),
)
