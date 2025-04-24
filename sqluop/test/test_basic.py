from sqluop import adaptor
from sqluop.adaptor import Base, create_engine, table_from_pydantic, Table, Index, AlchemyDatabase
from uopmeta.schemas import meta
def test_basics():
    metadata = Base.metadata
    db = AlchemyDatabase('foobar2')
    assert db
