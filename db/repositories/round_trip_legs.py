from ..models import RoundTripLeg

class RoundTripLegRepo:
    def __init__(self, session):
        self.session = session

    async def add(self, payload: dict) -> RoundTripLeg:
        obj = RoundTripLeg(**payload)
        self.session.add(obj)
        await self.session.flush()
        return obj
