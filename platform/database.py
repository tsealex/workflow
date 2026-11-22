from sqlalchemy.ext.asyncio import AsyncSession as SQLAlchemyAsyncSession


class AsyncSession(SQLAlchemyAsyncSession):
    """Roll back database changes when a commit fails."""

    async def commit(self) -> None:
        """Save changes or roll back and raise the commit error."""
        try:
            await super().commit()
        except Exception:
            await self.rollback()
            raise
