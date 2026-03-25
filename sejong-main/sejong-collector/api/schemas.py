"""Pydantic 응답 스키마 + status 매핑"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, model_validator


# 백엔드 4단계 → 프론트 3단계 매핑
FRONT_STATUS_MAP = {
    "collected": "unverified",
    "selected": "unverified",
    "verified": "verified",
    "excluded": "excluded",
}


class SummaryResponse(BaseModel):
    totalCollected: int
    jsonFileCount: int
    selectedActive: int
    selectedTarget: int
    verifiedCount: int
    verifiedRate: float
    excludedByDate: int
    excludedByPdf: int
    excludedByLang: int
    totalCountries: int
    totalOrgs: int
    completedDocs: int
    gbExcluded: bool
    gaoExcluded: bool


class CountryResponse(BaseModel):
    code: str
    name: str
    flag: str
    collected: int
    selected: int
    quota: int
    orgCount: int
    excluded: bool = False


class CategoryResponse(BaseModel):
    name: str
    nameKr: str
    count: int
    color: str
    icon: str


class DocumentResponse(BaseModel):
    id: int
    country: str
    countryFlag: str
    orgName: str
    orgCode: str
    title: str
    docType: str
    publishedDate: str
    relevanceScore: float
    status: Literal["verified", "unverified", "excluded"]
    pdfUrl: Optional[str] = None
    pdfSizeKb: Optional[int] = None
    authors: str
    description: str
    keywords: list[str]
    category: str
    brmCode1: str
    brmCode2: str
    journal: str
    volumeInfo: str
    isbn: str
    issn: str
    license: str
    dateVerified: bool
    dateVerifiedBy: str
    pdfVerified: bool
    langVerified: bool
    excludeReasons: list[str]
    link: Optional[str] = None

    @model_validator(mode="before")
    @classmethod
    def map_status(cls, data):
        if isinstance(data, dict) and "status" in data:
            data["status"] = FRONT_STATUS_MAP.get(data["status"], data["status"])
        return data


class DocumentListResponse(BaseModel):
    items: list[DocumentResponse]
    total: int
    page: int
    totalPages: int


class PipelineRunRequest(BaseModel):
    steps: Optional[list[int]] = None


class PipelineRunResponse(BaseModel):
    runId: int


class PipelineStatusResponse(BaseModel):
    currentStep: Optional[int] = None
    steps: dict[str, str]
    progress: float
    isRunning: bool
    # 최근 실행 결과
    runId: Optional[int] = None
    startedAt: Optional[str] = None
    finishedAt: Optional[str] = None
    finalStatus: Optional[str] = None
    totalCollected: int = 0
    totalSelected: int = 0
    totalVerified: int = 0
    totalExcluded: int = 0
    errorMessage: Optional[str] = None
    hasExcelFile: bool = False


class PipelineHistoryItem(BaseModel):
    id: int
    startedAt: Optional[str] = None
    finishedAt: Optional[str] = None
    status: str
    totalCollected: int = 0
    totalSelected: int = 0
    totalVerified: int = 0
    totalExcluded: int = 0
    errorMessage: Optional[str] = None


class QualityCheckResponse(BaseModel):
    label: str
    passed: bool
    detail: str


class SettingResponse(BaseModel):
    key: str
    value: str


class SettingUpdateRequest(BaseModel):
    key: str
    value: str


class HealthResponse(BaseModel):
    status: str
    db: bool
    version: str


# ─── Sites ───

class SiteResponse(BaseModel):
    id: int
    code: str
    name: Optional[str] = None
    nameKr: Optional[str] = None
    countryCode: Optional[str] = None
    country: Optional[str] = None
    orgType: Optional[str] = None
    acronym: Optional[str] = None
    url: Optional[str] = None
    brmCategory: Optional[str] = None
    currentUse: Optional[str] = None
    expectedCount: Optional[int] = None
    excluded: bool = False
    scheduleType: str = "manual"
    scheduleDays: Optional[str] = None
    scheduleTime: str = "03:00"
    lastCrawledAt: Optional[str] = None
    nextCrawlAt: Optional[str] = None
    crawlStatus: str = "idle"
    lastError: Optional[str] = None
    docCount: int = 0


class SiteListResponse(BaseModel):
    items: list[SiteResponse]
    total: int
    page: int
    totalPages: int


class SiteCreateRequest(BaseModel):
    code: str
    name: Optional[str] = None
    nameKr: Optional[str] = None
    countryCode: Optional[str] = None
    country: Optional[str] = None
    orgType: Optional[str] = None
    acronym: Optional[str] = None
    url: Optional[str] = None
    brmCategory: Optional[str] = None
    currentUse: Optional[str] = None
    expectedCount: Optional[int] = None
    excluded: bool = False
    scheduleType: str = "manual"
    scheduleDays: Optional[str] = None
    scheduleTime: str = "03:00"


class SiteUpdateRequest(BaseModel):
    name: Optional[str] = None
    nameKr: Optional[str] = None
    countryCode: Optional[str] = None
    country: Optional[str] = None
    orgType: Optional[str] = None
    acronym: Optional[str] = None
    url: Optional[str] = None
    brmCategory: Optional[str] = None
    currentUse: Optional[str] = None
    expectedCount: Optional[int] = None
    excluded: Optional[bool] = None
    scheduleType: Optional[str] = None
    scheduleDays: Optional[str] = None
    scheduleTime: Optional[str] = None
