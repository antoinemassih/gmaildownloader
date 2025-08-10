from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from ..models import Contract

IDENTITY_COLS = [
    "instrument_id","is_option","option_type","expiry_date","strike","root","multiplier"
]

class ContractRepo:
    def __init__(self, session):
        self.session = session

    async def find_or_create(self, **identity) -> Contract:
        stmt = (
            insert(Contract)
            .values(**identity)
            .on_conflict_do_nothing(index_elements=IDENTITY_COLS)
            .returning(Contract)
        )
        res = await self.session.execute(stmt)
        row = res.scalar_one_or_none()
        if row:
            return row
        # fallback select
        conds = [getattr(Contract, k) == v for k, v in identity.items()]
        res = await self.session.execute(select(Contract).where(*conds))
        return res.scalar_one()
