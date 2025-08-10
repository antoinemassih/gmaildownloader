from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from ..models import Instrument

class InstrumentRepo:
    def __init__(self, session):
        self.session = session

    async def find_or_create(self, symbol: str, asset_class: str) -> Instrument:
        stmt = (
            insert(Instrument)
            .values(symbol=symbol, asset_class=asset_class)
            .on_conflict_do_nothing(index_elements=[Instrument.symbol, Instrument.asset_class])
            .returning(Instrument)
        )
        res = await self.session.execute(stmt)
        row = res.scalar_one_or_none()
        if row:
            return row
        res = await self.session.execute(
            select(Instrument).where(Instrument.symbol == symbol, Instrument.asset_class == asset_class)
        )
        return res.scalar_one()
