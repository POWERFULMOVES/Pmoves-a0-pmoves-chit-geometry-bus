"""Geometry Bus health checker for PMOVES NATS event bus.

Exposes Agent Zero-callable tools for checking GEOMETRY BUS NATS subject health.
Wraps async NATS introspection logic; falls back gracefully when NATS is unreachable.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")

# All known GEOMETRY BUS subjects organized by domain
_SUBJECTS: Dict[str, List[str]] = {
    "tokenism.attribution": ["tokenism.attribution.recorded.v1"],
    "tokenism.cgp": ["tokenism.cgp.weekly.v1", "tokenism.cgp.ready.v1"],
    "tokenism.geometry": ["tokenism.geometry.event.v1"],
    "tokenism.swarm": ["tokenism.swarm.population.v1"],
    "tokenism.credential": ["tokenism.credential.rotated.v1"],
    "geometry.packet": ["geometry.packet.encoded.v1"],
    "hf.model": ["hf.model.downloaded.v1"],
    "research": ["research.deepresearch.request.v1", "research.deepresearch.result.v1"],
    "supaserch": ["supaserch.request.v1", "supaserch.result.v1"],
    "ingest": [
        "ingest.file.added.v1",
        "ingest.transcript.ready.v1",
        "ingest.summary.ready.v1",
        "ingest.chapters.ready.v1",
    ],
    "claude.code": ["claude.code.tool.executed.v1"],
    "skills.pipeline": [
        "skills.pipeline.model-benchmark-viz.v1",
        "skills.pipeline.ingest-chit-index.v1",
        "skills.pipeline.research-render.v1",
        "skills.pipeline.chit-3d-viz.v1",
        "skills.pipeline.voice-synthesis.v1",
        "skills.pipeline.agent-card-gen.v1",
    ],
}


@dataclass
class SubjectHealth:
    subject: str
    domain: str
    has_stream: bool = False
    consumer_count: int = 0
    message_count: int = 0
    status: str = "unknown"


@dataclass
class BusHealth:
    connected: bool = False
    jetstream_enabled: bool = False
    subjects: List[SubjectHealth] = field(default_factory=list)
    total_subjects: int = 0
    active_subjects: int = 0
    idle_subjects: int = 0
    missing_subjects: int = 0
    error: Optional[str] = None

    @property
    def health_pct(self) -> float:
        if self.total_subjects == 0:
            return 0.0
        return (self.active_subjects / self.total_subjects) * 100

    def to_dict(self) -> Dict[str, Any]:
        return {
            "connected": self.connected,
            "jetstream_enabled": self.jetstream_enabled,
            "total_subjects": self.total_subjects,
            "active_subjects": self.active_subjects,
            "idle_subjects": self.idle_subjects,
            "missing_subjects": self.missing_subjects,
            "health_pct": round(self.health_pct, 1),
            "error": self.error,
            "subjects": [
                {
                    "subject": s.subject,
                    "domain": s.domain,
                    "has_stream": s.has_stream,
                    "consumer_count": s.consumer_count,
                    "message_count": s.message_count,
                    "status": s.status,
                }
                for s in self.subjects
            ],
        }


async def _check_health_async(nats_url: str) -> BusHealth:
    health = BusHealth()
    all_subjects = [
        (domain, subj)
        for domain, subjects in _SUBJECTS.items()
        for subj in subjects
    ]
    health.total_subjects = len(all_subjects)

    try:
        import nats

        nc = await nats.connect(nats_url)
        health.connected = True
        js = nc.jetstream()
        health.jetstream_enabled = True

        for domain, subj in all_subjects:
            sh = SubjectHealth(subject=subj, domain=domain)
            try:
                stream_info = await js.find_stream_name_by_subject(subj)
                if stream_info:
                    sh.has_stream = True
                    sh.status = "active"
                    health.active_subjects += 1
                else:
                    sh.status = "no_stream"
                    health.missing_subjects += 1
            except Exception:
                sh.status = "no_stream"
                health.missing_subjects += 1
            health.subjects.append(sh)

        await nc.drain()
    except ImportError:
        health.error = "nats-py not installed — run: pip install nats-py"
    except Exception as exc:
        health.error = str(exc)

    return health


def check_bus_health(nats_url: str = "") -> Dict[str, Any]:
    """Check GEOMETRY BUS health. Returns JSON-serializable dict.

    Args:
        nats_url: NATS server URL (default:  env or nats://localhost:4222)

    Returns:
        Dict with connected, health_pct, active/idle/missing counts, per-subject status.
    """
    url = nats_url or NATS_URL
    health = asyncio.run(_check_health_async(url))
    return health.to_dict()


def get_subject_health(subject_pattern: str, nats_url: str = "") -> Dict[str, Any]:
    """Check health of subjects matching a pattern (prefix match).

    Args:
        subject_pattern: Prefix to match, e.g. "tokenism.cgp" or "ingest"
        nats_url: NATS server URL

    Returns:
        Dict with matched subjects and their health status.
    """
    url = nats_url or NATS_URL
    health = asyncio.run(_check_health_async(url))
    matched = [s for s in health.subjects if s.subject.startswith(subject_pattern)]
    return {
        "pattern": subject_pattern,
        "matched": len(matched),
        "subjects": [
            {"subject": s.subject, "domain": s.domain, "status": s.status}
            for s in matched
        ],
        "error": health.error,
    }
