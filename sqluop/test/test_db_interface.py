from uop.test import test_db_interface as tdi
from uop.test.testing import TestContext
from sqluop import adaptor
from uop import db_service
from uopmeta.schemas import meta

async def test_interface():
    db_service.DatabaseClass.register_db(adaptor.AlchemyDatabase, 'sqlite3')
    tdi.set_context(db_type='sqlite3')
    await tdi.test_db()

