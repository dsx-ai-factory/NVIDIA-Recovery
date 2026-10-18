# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from fastapi import APIRouter, HTTPException, Response

from notifications.dependencies import SessionDep
from notifications.models.preferences import (
    UserPreferencesOrm,
    UserPreferencesPublic,
    UserPreferencesPut,
)

router = APIRouter(tags=["preferences"])


@router.put(
    "/user-preferences/{username}",
    summary="Create or update a user's notification preferences.",
    responses={"201": {"description": "Successfully created.", "model": UserPreferencesPublic}},
)
async def user_preferences_put(
    username: str, preferences_payload: UserPreferencesPut, session: SessionDep, response: Response
) -> UserPreferencesPublic:
    preferences_dict = preferences_payload.model_dump()
    preferences = session.get(UserPreferencesOrm, username)
    if preferences is None:
        preferences = UserPreferencesOrm.model_validate({"user": username, **preferences_dict})
        response.status_code = 201
        session.add(preferences)
    else:
        # Update the row by replacing all the (writable) values.
        for attr, value in preferences_dict.items():
            setattr(preferences, attr, value)
    session.commit()
    session.refresh(preferences)
    return preferences


@router.get("/user-preferences/{username}", summary="Retrieve a user's notification preferences.")
async def user_preferences_get(username: str, session: SessionDep) -> UserPreferencesPublic:
    preferences = session.get(UserPreferencesOrm, username)
    if preferences is None:
        raise HTTPException(status_code=404)
    return preferences
