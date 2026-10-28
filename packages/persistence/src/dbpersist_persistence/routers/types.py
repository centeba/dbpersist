from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import Principal, get_principal, require_type
from dbpersist_persistence.db import get_session
from dbpersist_persistence.models import ObjectType
from dbpersist_persistence.schemas import ObjectTypeIn, ObjectTypeOut
from dbpersist_persistence.services.objects import get_type

router = APIRouter(prefix="/types", tags=["types"], dependencies=[Depends(get_principal)])


@router.post("", response_model=ObjectTypeOut, status_code=status.HTTP_201_CREATED)
async def create_type(
    body: ObjectTypeIn,
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> ObjectType:
    # registering a type is an admin act on its prefix, so a domain admin may do it
    require_type(principal, body.name, "admin")
    if await session.get(ObjectType, body.name) is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, f"type {body.name!r} already exists")
    otype = ObjectType(name=body.name, key_fields=body.key_fields, json_schema=body.json_schema)
    session.add(otype)
    await session.commit()
    return otype


@router.get("", response_model=list[ObjectTypeOut])
async def list_types(
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> list[ObjectType]:
    rows = (await session.execute(select(ObjectType).order_by(ObjectType.name))).scalars()
    return [t for t in rows if principal.allows(t.name, "read")]


@router.get("/{name}", response_model=ObjectTypeOut)
async def read_type(
    name: str,
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> ObjectType:
    require_type(principal, name, "read")
    return await get_type(session, name)
