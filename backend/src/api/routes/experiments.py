"""Experiment tracker API routes for RAG evaluation."""

import json
import logging
import random
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel as PydanticBaseModel
from sqlalchemy import select

from src.api.deps import DBSession
from src.models.domain import ExperimentModel, ExperimentResultModel
from src.services.database import SessionLocal

logger = logging.getLogger(__name__)

router = APIRouter()

# Path to test questions JSON
TEST_QUESTIONS_PATH = Path(__file__).parent.parent.parent / "data" / "test_questions.json"

SEARCH_MODES = ["v", "vg", "vw", "vgw"]

MODE_LABELS = {
    "v": "Weaviate Only",
    "vg": "Weaviate + Graph",
    "vw": "Weaviate + Web",
    "vgw": "Weaviate + Graph + Web",
}

# Set of experiment IDs that have been cancelled/deleted while running
_cancelled_experiments: set[int] = set()

# Module-level dict to pass config to background runner threads
_experiment_configs: dict[int, dict] = {}


def _load_test_questions() -> list[dict[str, Any]]:
    """Load test questions from the JSON file."""
    with open(TEST_QUESTIONS_PATH) as f:
        return json.load(f)


def _stratified_sample(questions: list[dict], percent: int) -> list[dict]:
    """Sample questions preserving category distribution."""
    by_category: dict[str, list[dict]] = defaultdict(list)
    for q in questions:
        by_category[q["category"]].append(q)

    sampled: list[dict] = []
    for category, cat_questions in by_category.items():
        n = max(1, round(len(cat_questions) * percent / 100))
        sampled.extend(random.sample(cat_questions, min(n, len(cat_questions))))

    random.shuffle(sampled)
    return sampled


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


class CreateExperimentRequest(PydanticBaseModel):
    name: str
    modes: list[str] | None = None  # subset of SEARCH_MODES; defaults to all
    sample_percent: int = 100  # 1-100, stratified sampling


@router.post("")
def create_experiment(request: CreateExperimentRequest, db: DBSession) -> dict[str, Any]:
    """Create a new experiment and launch it in a background thread."""
    name = request.name
    # Check for duplicate name
    existing = db.execute(
        select(ExperimentModel).where(ExperimentModel.name == name)
    ).scalar_one_or_none()
    if existing:
        raise HTTPException(status_code=409, detail=f"Experiment '{name}' already exists")

    questions = _load_test_questions()

    # Validate modes
    modes = request.modes or list(SEARCH_MODES)
    modes = [m for m in modes if m in SEARCH_MODES]
    if not modes:
        raise HTTPException(status_code=400, detail="No valid modes specified")

    # Stratified sampling
    if request.sample_percent < 100:
        questions = _stratified_sample(questions, request.sample_percent)

    experiment = ExperimentModel(
        name=name,
        status="pending",
        total_questions=len(questions),
        completed_questions=0,
        config={"modes": modes, "sample_percent": request.sample_percent},
    )
    db.add(experiment)
    db.commit()
    db.refresh(experiment)

    # Store config for the background runner
    _experiment_configs[experiment.id] = {
        "modes": modes,
        "questions": questions,
    }

    # Launch background thread (sync, not async)
    thread = threading.Thread(
        target=_run_experiment_background,
        args=(experiment.id,),
        daemon=True,
    )
    thread.start()

    return _experiment_to_dict(experiment)


@router.get("")
def list_experiments(db: DBSession) -> list[dict[str, Any]]:
    """List all experiments ordered by created_at descending."""
    stmt = select(ExperimentModel).order_by(ExperimentModel.created_at.desc())
    experiments = db.execute(stmt).scalars().all()
    return [_experiment_to_dict(exp) for exp in experiments]


@router.get("/questions")
def get_test_questions() -> list[dict[str, Any]]:
    """Return the full set of test questions."""
    return _load_test_questions()


@router.get("/modes")
def get_available_modes():
    """Return available search modes."""
    from src.services.web_search import is_available as web_available

    web_ok = web_available()
    modes = [
        {"id": "v", "label": "Weaviate Only", "available": True},
        {"id": "vg", "label": "Weaviate + Graph", "available": True},
        {"id": "vw", "label": "Weaviate + Web", "available": web_ok},
        {"id": "vgw", "label": "Weaviate + Graph + Web", "available": web_ok},
    ]
    return modes


@router.delete("/{experiment_id}")
def delete_experiment(experiment_id: int, db: DBSession) -> dict[str, str]:
    """Delete an experiment and all its results. Cancels if running."""
    experiment = db.get(ExperimentModel, experiment_id)
    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")
    # Signal the background runner to stop
    if experiment.status == "running":
        _cancelled_experiments.add(experiment_id)
    # Delete results then experiment
    db.query(ExperimentResultModel).filter(
        ExperimentResultModel.experiment_id == experiment_id
    ).delete()
    db.delete(experiment)
    db.commit()
    return {"status": "deleted"}


@router.get("/{experiment_id}")
def get_experiment(experiment_id: int, db: DBSession) -> dict[str, Any]:
    """Get experiment detail including aggregate metrics."""
    experiment = db.get(ExperimentModel, experiment_id)
    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")
    return _experiment_to_dict(experiment)


@router.get("/{experiment_id}/results")
def get_experiment_results(
    experiment_id: int,
    db: DBSession,
    category: str | None = Query(None),
    mode: str | None = Query(None),
) -> list[dict[str, Any]]:
    """Get individual results for an experiment, optionally filtered."""
    experiment = db.get(ExperimentModel, experiment_id)
    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")

    stmt = select(ExperimentResultModel).where(
        ExperimentResultModel.experiment_id == experiment_id
    )
    if category:
        stmt = stmt.where(ExperimentResultModel.category == category)
    if mode:
        stmt = stmt.where(ExperimentResultModel.mode == mode)

    results = db.execute(stmt).scalars().all()
    return [_result_to_dict(r) for r in results]


@router.get("/{experiment_id}/comparison")
def get_experiment_comparison(experiment_id: int, db: DBSession) -> list[dict[str, Any]]:
    """Side-by-side comparison of all modes per question. Supports N modes."""
    experiment = db.get(ExperimentModel, experiment_id)
    if not experiment:
        raise HTTPException(status_code=404, detail="Experiment not found")

    stmt = select(ExperimentResultModel).where(
        ExperimentResultModel.experiment_id == experiment_id
    )
    results = db.execute(stmt).scalars().all()

    # Group by question_id
    by_question: dict[str, dict[str, Any]] = {}
    for r in results:
        if r.question_id not in by_question:
            by_question[r.question_id] = {
                "question_id": r.question_id,
                "question_text": r.question_text,
                "category": r.category,
                "modes": {},
            }
        by_question[r.question_id]["modes"][r.mode] = _result_to_dict(r)

    # Determine winner (highest confidence)
    comparisons = []
    for qid, data in by_question.items():
        modes_data = data["modes"]
        winner = "tie"
        if modes_data:
            best_mode = max(modes_data.keys(), key=lambda m: modes_data[m]["confidence"])
            best_conf = modes_data[best_mode]["confidence"]
            # Check if there's a clear winner (margin > 0.01)
            others = [modes_data[m]["confidence"] for m in modes_data if m != best_mode]
            if others and best_conf > max(others) + 0.01:
                winner = best_mode
        data["winner"] = winner
        comparisons.append(data)

    return comparisons


# ---------------------------------------------------------------------------
# Background runner
# ---------------------------------------------------------------------------


def _run_experiment_background(experiment_id: int) -> None:
    """Run the experiment in a background thread.

    Uses its own SQLAlchemy session and sync httpx client.
    """
    # Retrieve config stored by create_experiment
    config = _experiment_configs.pop(experiment_id, {})
    modes = config.get("modes", SEARCH_MODES)
    questions = config.get("questions", _load_test_questions())

    db = SessionLocal()
    try:
        experiment = db.get(ExperimentModel, experiment_id)
        if not experiment:
            logger.error(f"Experiment {experiment_id} not found in background runner")
            return

        experiment.status = "running"
        experiment.started_at = datetime.now(timezone.utc)
        db.commit()

        all_results: list[ExperimentResultModel] = []

        with httpx.Client(timeout=120.0) as client:
            for i, question in enumerate(questions):
                # Check if experiment was cancelled/deleted
                if experiment_id in _cancelled_experiments:
                    _cancelled_experiments.discard(experiment_id)
                    logger.info(f"Experiment {experiment_id} cancelled")
                    return

                for mode in modes:
                    result_row = _run_single_question(
                        client, experiment_id, question, mode
                    )
                    db.add(result_row)
                    all_results.append(result_row)

                # Update progress after each question across all modes
                experiment.completed_questions = i + 1
                db.commit()

        # Compute aggregate metrics
        experiment.aggregate_metrics = _compute_aggregate_metrics(all_results, modes)
        experiment.status = "completed"
        experiment.completed_at = datetime.now(timezone.utc)
        db.commit()

    except Exception:
        logger.exception(f"Experiment {experiment_id} failed")
        try:
            experiment = db.get(ExperimentModel, experiment_id)
            if experiment:
                experiment.status = "failed"
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


def _run_single_question(
    client: httpx.Client,
    experiment_id: int,
    question: dict[str, Any],
    mode: str,
) -> ExperimentResultModel:
    """Execute a single question against the search API and return a result row."""
    start_ms = time.time()

    try:
        response = client.post(
            "http://localhost:8000/api/search/query",
            json={"query": question["question"], "mode": mode},
        )
        elapsed_ms = int((time.time() - start_ms) * 1000)

        if response.status_code != 200:
            logger.warning(
                f"Search API returned {response.status_code} for question "
                f"{question['id']} mode={mode}"
            )
            return _empty_result(experiment_id, question, mode, elapsed_ms)

        data = response.json()

    except Exception as exc:
        elapsed_ms = int((time.time() - start_ms) * 1000)
        logger.warning(f"Search API error for {question['id']} mode={mode}: {exc}")
        return _empty_result(experiment_id, question, mode, elapsed_ms)

    # Extract metrics from response
    answer = data.get("answer", "")
    confidence = float(data.get("confidence", 0.0))
    documents = data.get("documents", [])

    # Average relevance score from returned documents
    relevance_scores = [
        float(d.get("relevance_score", d.get("score", 0.0)))
        for d in documents
        if d.get("relevance_score", d.get("score")) is not None
    ]
    avg_relevance = (
        sum(relevance_scores) / len(relevance_scores) if relevance_scores else 0.0
    )

    # Entity coverage
    entity_coverage = _compute_entity_coverage(question, data)

    # Token usage
    usage = data.get("usage", {})
    input_tokens = int(usage.get("input_tokens", 0))
    output_tokens = int(usage.get("output_tokens", 0))
    total_tokens = input_tokens + output_tokens

    # Extract quality metrics from response
    metrics = data.get("metrics") or {}
    sts = float(metrics.get("sts", 0.0))
    nvs = float(metrics.get("nvs", 0.0))
    hds = int(metrics.get("hds", 0))
    cscs = float(metrics.get("cscs", 1.0))

    return ExperimentResultModel(
        experiment_id=experiment_id,
        question_id=question["id"],
        question_text=question["question"],
        category=question["category"],
        mode=mode,
        answer=answer,
        confidence=confidence,
        avg_relevance_score=avg_relevance,
        entity_coverage=entity_coverage,
        response_time_ms=elapsed_ms,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        total_tokens=total_tokens,
        documents_returned=len(documents),
        sts=sts,
        nvs=nvs,
        hds=hds,
        cscs=cscs,
        raw_result=data,
    )


def _empty_result(
    experiment_id: int,
    question: dict[str, Any],
    mode: str,
    elapsed_ms: int,
) -> ExperimentResultModel:
    """Return a zeroed-out result row for a failed question."""
    return ExperimentResultModel(
        experiment_id=experiment_id,
        question_id=question["id"],
        question_text=question["question"],
        category=question["category"],
        mode=mode,
        answer="",
        confidence=0.0,
        avg_relevance_score=0.0,
        entity_coverage=0.0,
        response_time_ms=elapsed_ms,
        input_tokens=0,
        output_tokens=0,
        total_tokens=0,
        documents_returned=0,
        sts=0.0,
        nvs=0.0,
        hds=0,
        cscs=1.0,
        raw_result=None,
    )


def _compute_entity_coverage(
    question: dict[str, Any], result: dict[str, Any]
) -> float:
    """Compute entity coverage: fraction of expected entities found in the answer/docs."""
    expected = set(e.lower() for e in question.get("expected_entities", []))
    if not expected:
        return 1.0

    found_in_answer: set[str] = set()
    answer_lower = result.get("answer", "").lower()
    for entity in expected:
        if entity in answer_lower:
            found_in_answer.add(entity)

    # Also check document snippets
    for doc in result.get("documents", []):
        snippet_lower = (
            doc.get("snippet", "") + " " + doc.get("document_title", "")
        ).lower()
        for entity in expected:
            if entity in snippet_lower:
                found_in_answer.add(entity)

    return len(found_in_answer) / len(expected)


def _compute_aggregate_metrics(
    results: list[ExperimentResultModel],
    modes: list[str] | None = None,
) -> dict[str, Any]:
    """Compute aggregate metrics from all experiment results.

    Handles N modes dynamically instead of assuming exactly 2.
    """

    def _mean(values: list[float]) -> float:
        return sum(values) / len(values) if values else 0.0

    def _mode_metrics(rows: list[ExperimentResultModel]) -> dict[str, float]:
        return {
            "mean_confidence": _mean([r.confidence for r in rows]),
            "mean_relevance": _mean([r.avg_relevance_score for r in rows]),
            "mean_entity_coverage": _mean([r.entity_coverage for r in rows]),
            "mean_response_time_ms": _mean([float(r.response_time_ms) for r in rows]),
            "mean_tokens": _mean([float(r.total_tokens) for r in rows]),
            "mean_sts": _mean([r.sts for r in rows]),
            "mean_nvs": _mean([r.nvs for r in rows]),
            "mean_hds": _mean([float(r.hds) for r in rows]),
            "mean_cscs": _mean([r.cscs for r in rows]),
        }

    # Discover all modes present in results
    all_modes = modes or list({r.mode for r in results})

    # Per-mode results
    results_by_mode: dict[str, list[ExperimentResultModel]] = {
        m: [r for r in results if r.mode == m] for m in all_modes
    }

    # Per-mode metrics
    per_mode: dict[str, dict] = {}
    for m in all_modes:
        per_mode[m] = _mode_metrics(results_by_mode[m])

    # Build per-question lookup for each mode
    by_q_by_mode: dict[str, dict[str, ExperimentResultModel]] = {}
    for m in all_modes:
        by_q_by_mode[m] = {r.question_id: r for r in results_by_mode[m]}

    # Pairwise wins across all modes
    all_question_ids = {r.question_id for r in results}
    wins: dict[str, int] = {m: 0 for m in all_modes}
    wins["tie"] = 0

    for qid in all_question_ids:
        confs = {}
        for m in all_modes:
            r = by_q_by_mode[m].get(qid)
            if r is not None:
                confs[m] = r.confidence

        if not confs:
            continue

        best_mode = max(confs, key=confs.get)
        best_conf = confs[best_mode]
        others = [c for m, c in confs.items() if m != best_mode]
        if others and best_conf > max(others) + 0.01:
            wins[best_mode] += 1
        else:
            wins["tie"] += 1

    # Per-category breakdown
    categories: set[str] = {r.category for r in results}
    by_category: dict[str, Any] = {}
    for cat in sorted(categories):
        cat_per_mode: dict[str, dict] = {}
        cat_results_by_mode: dict[str, list] = {}
        for m in all_modes:
            cat_rows = [r for r in results_by_mode[m] if r.category == cat]
            cat_results_by_mode[m] = cat_rows
            cat_per_mode[m] = _mode_metrics(cat_rows)

        cat_wins: dict[str, int] = {m: 0 for m in all_modes}
        cat_wins["tie"] = 0
        cat_question_ids = {r.question_id for r in results if r.category == cat}

        for qid in cat_question_ids:
            confs = {}
            for m in all_modes:
                cat_by_q = {r.question_id: r for r in cat_results_by_mode[m]}
                r = cat_by_q.get(qid)
                if r is not None:
                    confs[m] = r.confidence

            if not confs:
                continue

            best_mode = max(confs, key=confs.get)
            best_conf = confs[best_mode]
            others = [c for m_key, c in confs.items() if m_key != best_mode]
            if others and best_conf > max(others) + 0.01:
                cat_wins[best_mode] += 1
            else:
                cat_wins["tie"] += 1

        by_category[cat] = {"by_mode": cat_per_mode, "wins": cat_wins}

    return {
        "by_mode": per_mode,
        "wins": wins,
        "by_category": by_category,
    }


# ---------------------------------------------------------------------------
# Serialization helpers
# ---------------------------------------------------------------------------


def _experiment_to_dict(exp: ExperimentModel) -> dict[str, Any]:
    return {
        "id": exp.id,
        "name": exp.name,
        "status": exp.status,
        "total_questions": exp.total_questions,
        "completed_questions": exp.completed_questions,
        "started_at": exp.started_at.isoformat() if exp.started_at else None,
        "completed_at": exp.completed_at.isoformat() if exp.completed_at else None,
        "aggregate_metrics": exp.aggregate_metrics,
        "config": exp.config,
        "created_at": exp.created_at.isoformat() if exp.created_at else None,
        "updated_at": exp.updated_at.isoformat() if exp.updated_at else None,
    }


def _result_to_dict(r: ExperimentResultModel) -> dict[str, Any]:
    return {
        "id": r.id,
        "experiment_id": r.experiment_id,
        "question_id": r.question_id,
        "question_text": r.question_text,
        "category": r.category,
        "mode": r.mode,
        "answer": r.answer,
        "confidence": r.confidence,
        "avg_relevance_score": r.avg_relevance_score,
        "entity_coverage": r.entity_coverage,
        "response_time_ms": r.response_time_ms,
        "input_tokens": r.input_tokens,
        "output_tokens": r.output_tokens,
        "total_tokens": r.total_tokens,
        "documents_returned": r.documents_returned,
        "sts": r.sts,
        "nvs": r.nvs,
        "hds": r.hds,
        "cscs": r.cscs,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }
