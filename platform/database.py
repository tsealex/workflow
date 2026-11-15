from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    metadata = MetaData(
        naming_convention={"ck": "%(table_name)s_%(column_0_name)s_check"}
    )
