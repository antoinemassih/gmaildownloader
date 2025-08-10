from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from ..models import Account

class AccountRepo:
    def __init__(self, session):
        self.session = session

    async def get_by_broker_code(self, broker_code: str) -> Account | None:
        res = await self.session.execute(select(Account).where(Account.broker_code == broker_code))
        return res.scalar_one_or_none()

    async def upsert(self, broker_code: str, display_name: str | None = None) -> Account:
        stmt = (
            insert(Account)
            .values(broker_code=broker_code, display_name=display_name)
            .on_conflict_do_update(
                index_elements=[Account.broker_code],
                set_={"display_name": display_name},
            )
            .returning(Account)
        )
        res = await self.session.execute(stmt)
        return res.scalar_one()
