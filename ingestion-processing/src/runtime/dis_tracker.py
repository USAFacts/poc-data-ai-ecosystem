"""DIS (Data Ingestion Score) tracking for pipeline executions.

Calculates and saves DIS scores after each workflow execution,
enabling trend tracking between actual pipeline runs.
"""

from datetime import datetime, timezone

from logging_manager import get_logger
from runtime.executor import ExecutionResult
from steps.base import StepStatus

logger = get_logger(__name__)

# DIS component weights
QUALITY_WEIGHT = 0.40
EFFICIENCY_WEIGHT = 0.30
EXECUTION_SUCCESS_WEIGHT = 0.30

# Target duration for efficiency calculation (5 minutes)
TARGET_DURATION_MS = 5 * 60 * 1000


def calculate_workflow_dis(result: ExecutionResult) -> dict:
    """Calculate DIS components for a completed workflow execution.

    Args:
        result: The execution result from running the workflow.

    Returns:
        Dictionary with DIS components:
        - dis_score: Overall DIS score (0-100)
        - quality_score: Quality component (0-100)
        - efficiency_score: Efficiency component (0-100)
        - execution_success_score: Execution success component (0-100)
    """
    # Calculate execution success (% of steps that succeeded)
    total_steps = len(result.step_results)
    successful_steps = sum(
        1 for r in result.step_results.values()
        if r.status == StepStatus.SUCCESS
    )
    execution_success_score = (successful_steps / total_steps * 100) if total_steps > 0 else 0

    # Calculate efficiency based on duration
    if result.started_at and result.completed_at:
        duration_ms = (result.completed_at - result.started_at).total_seconds() * 1000
        # Efficiency: 100 if instant, decreases as duration increases
        # At target time (5 min): 75 points
        # At 2x target: ~56 points
        efficiency_score = 100 * (TARGET_DURATION_MS / (TARGET_DURATION_MS + duration_ms))
    else:
        efficiency_score = 75.0  # Default if no timing available

    # Extract quality score from parse step output
    quality_score = 0.0
    for step_name, step_result in result.step_results.items():
        if step_result.output and isinstance(step_result.output, dict):
            quality = step_result.output.get("quality", {})
            if quality:
                # Try the step output format first (overall_score)
                overall = quality.get("overall_score")
                if overall is None:
                    # Fall back to nested format (scores.overall)
                    scores = quality.get("scores", {})
                    overall = scores.get("overall")
                if overall is not None:
                    quality_score = overall
                    break

    # Calculate composite DIS score
    dis_score = (
        quality_score * QUALITY_WEIGHT +
        efficiency_score * EFFICIENCY_WEIGHT +
        execution_success_score * EXECUTION_SUCCESS_WEIGHT
    )

    return {
        "dis_score": dis_score,
        "quality_score": quality_score,
        "efficiency_score": efficiency_score,
        "execution_success_score": execution_success_score,
    }


def save_workflow_dis(workflow_name: str, dis_data: dict) -> bool:
    """Save DIS score for a workflow execution to the database.

    Args:
        workflow_name: Name of the workflow.
        dis_data: DIS data from calculate_workflow_dis().

    Returns:
        True if saved successfully, False otherwise.
    """
    try:
        from db.database import get_engine, init_db, get_session
        from db.repository import DISHistoryRepository

        engine = get_engine()
        init_db(engine)

        with get_session(engine) as session:
            repo = DISHistoryRepository(session)
            repo.record_workflow_dis(
                workflow_name=workflow_name,
                dis_score=dis_data["dis_score"],
                quality_score=dis_data["quality_score"],
                efficiency_score=dis_data["efficiency_score"],
                execution_success_score=dis_data["execution_success_score"],
            )
            session.commit()

        logger.debug(f"Saved DIS score for {workflow_name}: {dis_data['dis_score']:.1f}")
        return True

    except Exception as e:
        logger.debug(f"Could not save DIS score for {workflow_name}: {e}")
        return False


def track_workflow_dis(workflow_name: str, result: ExecutionResult) -> dict | None:
    """Calculate and save DIS for a workflow execution.

    This is the main entry point called after workflow execution.

    Args:
        workflow_name: Name of the workflow.
        result: The execution result.

    Returns:
        DIS data dict if successful, None otherwise.
    """
    if result.status != "success":
        logger.debug(f"Skipping DIS tracking for failed workflow: {workflow_name}")
        return None

    dis_data = calculate_workflow_dis(result)
    save_workflow_dis(workflow_name, dis_data)

    return dis_data


def update_overall_dis() -> bool:
    """Update the overall DIS score based on latest per-workflow scores.

    This aggregates the most recent DIS score for each workflow
    and saves an overall DIS record.

    Returns:
        True if saved successfully, False otherwise.
    """
    try:
        from sqlalchemy import select, func
        from db.database import get_engine, init_db, get_session
        from db.models import DISHistoryModel
        from db.repository import DISHistoryRepository

        engine = get_engine()
        init_db(engine)

        with get_session(engine) as session:
            # Get latest DIS for each workflow
            subquery = (
                select(
                    DISHistoryModel.workflow_name,
                    func.max(DISHistoryModel.recorded_at).label("max_recorded_at")
                )
                .group_by(DISHistoryModel.workflow_name)
                .subquery()
            )

            stmt = (
                select(DISHistoryModel)
                .join(
                    subquery,
                    (DISHistoryModel.workflow_name == subquery.c.workflow_name) &
                    (DISHistoryModel.recorded_at == subquery.c.max_recorded_at)
                )
            )

            records = list(session.execute(stmt).scalars().all())

            if not records:
                return False

            # Calculate averages
            workflow_count = len(records)
            avg_dis = sum(r.dis_score for r in records) / workflow_count
            avg_quality = sum(r.quality_score for r in records) / workflow_count
            avg_efficiency = sum(r.efficiency_score for r in records) / workflow_count
            avg_execution_success = sum(r.execution_success_score for r in records) / workflow_count

            # Save overall DIS
            repo = DISHistoryRepository(session)
            repo.record_overall_dis(
                overall_dis=avg_dis,
                avg_quality=avg_quality,
                avg_efficiency=avg_efficiency,
                avg_execution_success=avg_execution_success,
                workflow_count=workflow_count,
            )
            session.commit()

        logger.debug(f"Updated overall DIS: {avg_dis:.1f} ({workflow_count} workflows)")
        return True

    except Exception as e:
        logger.debug(f"Could not update overall DIS: {e}")
        return False
