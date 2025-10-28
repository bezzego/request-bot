from __future__ import annotations

from typing import Type

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db.models import (
    Customer,
    Engineer,
    Leader,
    Master,
    Specialist,
    User,
    UserRole,
)


class UserRoleService:
    """Управление ролями пользователей и профилями."""

    ROLE_MODEL_MAP: dict[UserRole, Type] = {
        UserRole.SPECIALIST: Specialist,
        UserRole.ENGINEER: Engineer,
        UserRole.MASTER: Master,
        UserRole.MANAGER: Leader,
        UserRole.CLIENT: Customer,
    }

    @staticmethod
    async def assign_role(session: AsyncSession, user: User, new_role: UserRole) -> None:
        if user.role == new_role:
            return

        # удалить старый профиль
        old_model = UserRoleService.ROLE_MODEL_MAP.get(user.role)
        if old_model:
            await session.execute(
                delete(old_model).where(old_model.user_id == user.id)
            )

        # создать новый профиль
        profile_model = UserRoleService.ROLE_MODEL_MAP.get(new_role)
        if profile_model:
            profile = profile_model(user_id=user.id)
            session.add(profile)

        user.role = new_role
        await session.flush()

    @staticmethod
    async def ensure_profile(session: AsyncSession, user: User) -> None:
        """Проверяет, что профиль текущей роли существует."""
        profile_model = UserRoleService.ROLE_MODEL_MAP.get(user.role)
        if not profile_model:
            return
        stmt = select(profile_model).where(profile_model.user_id == user.id)
        result = await session.execute(stmt)
        if not result.scalars().first():
            session.add(profile_model(user_id=user.id))
            await session.flush()

    @staticmethod
    async def set_super_admin(session: AsyncSession, user: User, value: bool) -> None:
        """Обновляет статус супер-админа (только для менеджеров)."""
        if value and user.role != UserRole.MANAGER:
            await UserRoleService.assign_role(session, user, UserRole.MANAGER)

        stmt = select(Leader).where(Leader.user_id == user.id)
        leader = (await session.execute(stmt)).scalars().first()

        if value:
            if leader:
                if not leader.is_super_admin:
                    leader.is_super_admin = True
            else:
                session.add(Leader(user_id=user.id, is_super_admin=True))
        else:
            if leader and leader.is_super_admin:
                leader.is_super_admin = False

        await session.flush()
