"""설정 API — GET/PUT /settings"""

import json
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from api.database import get_db
from api.models import Setting
from api.schemas import SettingResponse, SettingUpdateRequest

router = APIRouter()
CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config.json"


@router.get("", response_model=list[SettingResponse])
async def get_settings(db: Session = Depends(get_db)):
    settings = db.query(Setting).all()
    return [SettingResponse(key=s.key, value=s.value) for s in settings]


@router.put("")
async def update_setting(req: SettingUpdateRequest, db: Session = Depends(get_db)):
    existing = db.query(Setting).filter(Setting.key == req.key).first()
    if existing:
        existing.value = req.value
        existing.updated_at = datetime.now()
    else:
        db.add(Setting(key=req.key, value=req.value))
    db.commit()

    # config.json 동기화
    _sync_to_config(db)

    return {"ok": True, "key": req.key, "value": req.value}


def _sync_to_config(db: Session):
    """settings 테이블 → config.json 동기화"""
    settings = {s.key: s.value for s in db.query(Setting).all()}

    # 기존 config.json 로드 (있으면 유지, 없으면 기본값)
    try:
        config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        config = {}

    # 설정값 반영
    int_keys = ["target_count", "min_pdf_size_kb", "min_title_length", "min_keyword_count", "org_cap_percent"]
    bool_keys = ["gb_excluded", "gao_excluded"]

    for key, value in settings.items():
        if key in int_keys:
            try:
                config[key] = int(value)
            except ValueError:
                config[key] = value
        elif key in bool_keys:
            config[key] = value.lower() == "true"
        elif key == "cutoff_date":
            config[key] = value
        elif key == "country_quota":
            try:
                config[key] = json.loads(value)
            except (json.JSONDecodeError, TypeError):
                pass

    CONFIG_PATH.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")
