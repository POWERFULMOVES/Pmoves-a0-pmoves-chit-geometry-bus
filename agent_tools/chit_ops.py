"""CHIT packet encode/decode and geometry bus event publishing.

Pure-Python CHIT encoding (no ML deps). Inlines minimal logic from
PMOVES chit_encode_hook.py for self-contained A0 plugin use.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


NATS_URL = os.environ.get("NATS_URL", "nats://localhost:4222")
ZETA_ZEROS = [14.134725, 21.022040, 25.010858, 30.424876, 32.935062, 37.586178]


@dataclass
class CGPPacket:
    version: str = "cgp.v2"
    timestamp: str = ""
    payload: Dict[str, Any] = field(default_factory=dict)
    checksum: str = ""
    source: str = "a0-chit-geometry-bus"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _spectral_signature(text: str) -> List[float]:
    h = int(hashlib.sha256(text.encode()).hexdigest()[:16], 16)
    return [round(z * (1.0 + ((h >> (i * 8)) & 0xFF) / 255.0 * 0.1), 6)
            for i, z in enumerate(ZETA_ZEROS)]


def _hyperbolic_coords(text: str, depth: float = 0.5) -> Dict[str, float]:
    angle = (int(hashlib.sha256(text[:200].encode()).hexdigest()[:8], 16) / 0xFFFFFFFF) * 2 * math.pi
    radius = min(0.95, depth * 0.95)
    return {"x": round(radius * math.cos(angle), 6), "y": round(radius * math.sin(angle), 6),
            "curvature": -1.0, "radius": round(radius, 6)}


def _dirichlet_weights(text: str) -> Dict[str, Any]:
    cats = ["factual", "conceptual", "procedural", "contextual", "relational"]
    words = text.lower().split()
    wc = max(1, len(words))
    alphas = [round(max(0.1, 0.1 + sum(1 for w in words if w in bucket) / wc * 12), 4)
              for bucket in (
                  {"number", "date", "fact", "data"},
                  {"concept", "theory", "model", "framework", "paradigm"},
                  {"step", "install", "run", "execute", "configure", "build", "deploy"},
                  {"when", "where", "environment", "context", "condition", "scenario"},
                  {"connects", "relates", "depends", "requires", "integrates"},
              )]
    total = sum(alphas)
    return {"alphas": alphas, "categories": cats, "total_alpha": round(total, 4)}


def encode_chit(
    text: str,
    metadata: Optional[Dict[str, Any]] = None,
    depth: float = 0.5,
) -> Dict[str, Any]:
    """Encode content as a CHIT Geometry Packet (CGP v2).

    Args:
        text: Content to encode.
        metadata: Optional metadata dict (source, tags, etc.).
        depth: Hierarchical depth [0.0, 1.0] for Poincare disk positioning.

    Returns:
        CGP v2 packet as JSON-serializable dict.
    """
    payload = {
        "content_hash": hashlib.sha256(text.encode()).hexdigest()[:16],
        "content_length": len(text),
        "word_count": len(text.split()),
        "hyperbolic_coords": _hyperbolic_coords(text, depth),
        "spectral_signature": _spectral_signature(text),
        "dirichlet_weights": _dirichlet_weights(text),
        "metadata": metadata or {},
    }
    checksum = hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()
    return CGPPacket(
        timestamp=datetime.now(timezone.utc).isoformat(),
        payload=payload,
        checksum=checksum,
    ).to_dict()


def decode_chit(cgp: Dict[str, Any]) -> Dict[str, Any]:
    """Verify and summarize a CGP packet (geometry-only decode — no corpus retrieval).

    Args:
        cgp: CGP packet dict as returned by encode_chit.

    Returns:
        Dict with verified, content_hash, content_length, checksum_ok, spectral_peak.
    """
    payload = cgp.get("payload", {})
    payload_json = json.dumps(payload, sort_keys=True)
    expected_checksum = hashlib.sha256(payload_json.encode()).hexdigest()
    checksum_ok = cgp.get("checksum") == expected_checksum

    spectral = payload.get("spectral_signature", [])
    return {
        "verified": checksum_ok,
        "content_hash": payload.get("content_hash"),
        "content_length": payload.get("content_length"),
        "word_count": payload.get("word_count"),
        "checksum_ok": checksum_ok,
        "spectral_peak": max(spectral) if spectral else None,
        "hyperbolic_radius": payload.get("hyperbolic_coords", {}).get("radius"),
        "metadata": payload.get("metadata", {}),
    }


async def _publish_async(subject: str, payload: Dict[str, Any], nats_url: str) -> bool:
    try:
        import nats as natspy
        nc = await natspy.connect(nats_url)
        await nc.publish(subject, json.dumps(payload).encode("utf-8"))
        await nc.drain()
        return True
    except Exception as exc:
        import sys
        sys.stderr.write(f"[chit_ops] NATS publish to {subject} failed: {exc}\n")
        return False


def publish_geometry_event(
    subject: str,
    payload: Dict[str, Any],
    nats_url: str = "",
) -> bool:
    """Publish an event to the PMOVES geometry bus.

    Args:
        subject: NATS subject, e.g. "geometry.packet.encoded.v1".
        payload: Event payload dict (must be JSON-serializable).
        nats_url: NATS server URL (default:  or nats://localhost:4222).

    Returns:
        True on successful publish, False otherwise.
    """
    url = nats_url or NATS_URL
    return asyncio.run(_publish_async(subject, payload, url))
