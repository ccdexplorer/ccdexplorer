from pathlib import Path
from ccdexplorer.api.app.factory import create_app, AppSettings
from ccdexplorer.grpc_client.core import GRPCClient
from ccdexplorer.mongodb.core import MongoDB, MongoMotor
from ccdexplorer.tooter.core import Tooter


HERE = Path(__file__).resolve().parent

grpcclient = GRPCClient()
tooter = Tooter()

mongodb = MongoDB(tooter, nearest=True)
motormongo = MongoMotor(tooter)
app_settings = AppSettings(
    static_dir=HERE / "static",
    templates_dir=HERE / "templates",
    node_modules_dir=HERE / "node_modules",
    mongo_factory=lambda: mongodb,
    motor_factory=lambda: motormongo,
    grpc_factory=lambda: grpcclient,
    tooter_factory=lambda: tooter,
)
app = create_app(app_settings)
