from sqlalchemy import select
from ..models import RoundTrip

class RoundTripRepo:
    def __init__(self, session):
        self.session = session

    async def get(self, round_trip_id):
        res = await self.session.execute(select(RoundTrip).where(RoundTrip.round_trip_id == round_trip_id))
        return res.scalar_one_or_none()

    async def create(self, payload: dict) -> RoundTrip:
        obj = RoundTrip(**payload)
        self.session.add(obj)
        await self.session.flush()
        return obj
