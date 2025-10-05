from pathlib import Path
from ccdexplorer.site.app.factory import create_app, AppSettings


HERE = Path(__file__).resolve().parent

app_settings = AppSettings(
    static_dir=HERE / "static",
    templates_dir=HERE / "templates",
    node_modules_dir=HERE / "node_modules",
    addresses_dir=HERE / "addresses",
)
app = create_app(app_settings)
