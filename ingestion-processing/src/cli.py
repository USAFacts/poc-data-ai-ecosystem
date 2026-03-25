"""CLI for government data pipeline."""

import json
import os
from pathlib import Path

# Must be set before any HuggingFace tokenizers import to prevent
# Rust thread pool from spawning (causes semaphore leaks on macOS).
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

import typer
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table

from control.registry import Registry, RegistryError
from control.compiler import Compiler
from control.validator import ConstraintValidator
from logging_manager import configure as configure_logging, get_logger
from runtime.context import ExecutionContext
from runtime.sequential import SequentialExecutor
from storage import MinioStorage, StorageError
from reports import generate_html_report

# Load environment variables
load_dotenv()

# Initialize centralized logging (console + optional file + optional DB)
configure_logging(
    level=os.getenv("PIPELINE_LOG_LEVEL", "INFO"),
    log_dir=os.getenv("PIPELINE_LOG_DIR"),
)
logger = get_logger(__name__)

app = typer.Typer(
    name="pipeline",
    help="Government data ingestion pipeline CLI",
    no_args_is_help=True,
)
db_app = typer.Typer(help="Database operations for manifest management")
app.add_typer(db_app, name="db")
logs_app = typer.Typer(help="Query pipeline logs stored in the database")
app.add_typer(logs_app, name="logs")

console = Console()


def get_manifests_path() -> Path:
    """Get manifests path from env or default."""
    path = os.getenv("MANIFESTS_PATH", "./manifests")
    return Path(path)


def load_registry() -> Registry:
    """Load and return the registry."""
    manifests_path = get_manifests_path()
    if not manifests_path.exists():
        console.print(f"[red]Manifests directory not found: {manifests_path}[/red]")
        raise typer.Exit(1)

    registry = Registry(manifests_path)
    registry.load()
    return registry


@app.command()
def validate(
    workflow: str | None = typer.Argument(
        None, help="Workflow name to validate (validates all if not specified)"
    ),
) -> None:
    """Validate manifest files and workflow configurations."""
    try:
        registry = load_registry()

        console.print(f"[green]✓ Loaded {len(registry.agencies)} agencies[/green]")
        console.print(f"[green]✓ Loaded {len(registry.assets)} assets[/green]")
        console.print(f"[green]✓ Loaded {len(registry.workflows)} workflows[/green]")

        compiler = Compiler(registry)
        validator = ConstraintValidator()

        if workflow:
            workflows_to_validate = [workflow]
        else:
            workflows_to_validate = list(registry.workflows.keys())

        all_valid = True
        for wf_name in workflows_to_validate:
            try:
                # Compile the workflow into an execution plan
                plan = compiler.compile(wf_name)

                # Validate constraints
                validation_result = validator.validate_plan(plan)

                if validation_result.valid:
                    console.print(f"[green]✓ Workflow '{wf_name}' is valid[/green]")
                else:
                    all_valid = False
                    console.print(f"[red]✗ Workflow '{wf_name}':[/red]")
                    for error in validation_result.errors:
                        console.print(f"  - {error}")
                    for warning in validation_result.warnings:
                        console.print(f"  [yellow]⚠ {warning}[/yellow]")

            except Exception as e:
                all_valid = False
                console.print(f"[red]✗ Workflow '{wf_name}': {e}[/red]")

        if all_valid:
            console.print("\n[green]All validations passed![/green]")
        else:
            raise typer.Exit(1)

    except RegistryError as e:
        console.print(f"[red]Registry error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def run(
    workflow: str | None = typer.Argument(None, help="Workflow name to run (omit for --all)"),
    all_workflows: bool = typer.Option(False, "--all", "-a", help="Run all workflows"),
    dry_run: bool = typer.Option(False, "--dry-run", "-n", help="Validate without executing"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output result as JSON"),
    parallel: int = typer.Option(1, "--parallel", "-p", help="Number of parallel workers (1=sequential)"),
) -> None:
    """Run a workflow pipeline or all workflows.

    Use --parallel to run multiple workflows concurrently:
        uv run pipeline run --all --parallel 3
    """
    try:
        registry = load_registry()
        storage = MinioStorage()
        compiler = Compiler(registry)

        # Determine which workflows to run
        if all_workflows:
            workflows_to_run = list(registry.workflows.keys())
            if not output_json:
                parallel_msg = f" (parallel: {parallel})" if parallel > 1 else ""
                console.print(f"[bold]Running all {len(workflows_to_run)} workflows{parallel_msg}...[/bold]\n")
        elif workflow:
            workflows_to_run = [workflow]
        else:
            console.print("[red]Error: Specify a workflow name or use --all[/red]")
            raise typer.Exit(1)

        # Use parallel execution if requested and multiple workflows
        if parallel > 1 and len(workflows_to_run) > 1:
            all_results, failed_count = _run_workflows_parallel(
                workflows_to_run, registry, storage, compiler, dry_run, output_json, parallel,
            )
        else:
            all_results, failed_count = _run_workflows_sequential(
                workflows_to_run, registry, storage, compiler, dry_run, output_json, all_workflows,
            )

        if output_json:
            if len(all_results) == 1:
                console.print(json.dumps(all_results[0].to_dict(), indent=2))
            else:
                console.print(json.dumps([r.to_dict() for r in all_results], indent=2))
        elif all_workflows:
            success_count = len(workflows_to_run) - failed_count
            console.print(f"[bold]Summary: {success_count}/{len(workflows_to_run)} succeeded[/bold]")

        if failed_count > 0:
            raise typer.Exit(1)

    except RegistryError as e:
        console.print(f"[red]Registry error: {e}[/red]")
        raise typer.Exit(1)
    except StorageError as e:
        console.print(f"[red]Storage error: {e}[/red]")
        raise typer.Exit(1)


def _run_workflows_sequential(
    workflows_to_run: list[str],
    registry: Registry,
    storage: MinioStorage,
    compiler: Compiler,
    dry_run: bool,
    output_json: bool,
    all_workflows: bool,
) -> tuple[list, int]:
    """Run workflows sequentially."""
    executor = SequentialExecutor(console if not output_json else None)
    all_results = []
    failed_count = 0

    for wf_name in workflows_to_run:
        if all_workflows and not output_json:
            console.print(f"[bold cyan]Running: {wf_name}[/bold cyan]")

        try:
            plan = compiler.compile(wf_name)
            context = ExecutionContext(plan=plan, storage=storage)
            result = executor.execute(plan, context, dry_run=dry_run)
            all_results.append(result)

            if output_json:
                continue

            if result.success:
                _print_workflow_success(wf_name, result)
            else:
                failed_count += 1
                console.print(f"[red]✗ Workflow '{wf_name}' failed[/red]")
                if result.error:
                    console.print(f"  Error: {result.error}")

        except Exception as e:
            failed_count += 1
            if not output_json:
                console.print(f"[red]✗ Workflow '{wf_name}' failed: {e}[/red]")

        if all_workflows and not output_json:
            console.print()

    return all_results, failed_count


def _run_workflows_parallel(
    workflows_to_run: list[str],
    registry: Registry,
    storage: MinioStorage,
    compiler: Compiler,
    dry_run: bool,
    output_json: bool,
    max_workers: int,
) -> tuple[list, int]:
    """Run workflows in parallel using ThreadPoolExecutor."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from threading import Lock

    all_results = []
    failed_count = 0
    results_lock = Lock()
    print_lock = Lock()

    def run_single_workflow(wf_name: str) -> tuple[str, any, str | None]:
        """Run a single workflow and return (name, result, error)."""
        try:
            executor = SequentialExecutor(console=None)
            plan = compiler.compile(wf_name)
            context = ExecutionContext(plan=plan, storage=storage)
            result = executor.execute(plan, context, dry_run=dry_run, show_progress=False)
            return (wf_name, result, None)
        except Exception as e:
            return (wf_name, None, str(e))

    # Show progress header
    if not output_json:
        console.print(f"[dim]Starting {len(workflows_to_run)} workflows with {max_workers} parallel workers...[/dim]\n")

    # Submit all workflows to the thread pool
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(run_single_workflow, wf): wf for wf in workflows_to_run}

        for future in as_completed(futures):
            wf_name, result, error = future.result()

            with results_lock:
                if result:
                    all_results.append(result)
                if error or (result and not result.success):
                    failed_count += 1

            # Print results as they complete
            if not output_json:
                with print_lock:
                    if error:
                        console.print(f"[red]✗ {wf_name}: {error}[/red]")
                    elif result and result.success:
                        duration = f" ({result.duration_seconds:.1f}s)" if result.duration_seconds else ""
                        # Get quality score if available
                        quality_info = ""
                        for step_result in result.step_results.values():
                            if step_result.output.get("quality"):
                                score = step_result.output["quality"].get("overall_score", 0)
                                quality_info = f" | Quality: {score:.1f}"
                                break
                        console.print(f"[green]✓ {wf_name}{duration}{quality_info}[/green]")
                    elif result:
                        console.print(f"[red]✗ {wf_name}: {result.error}[/red]")

    if not output_json:
        console.print()

    return all_results, failed_count


def _print_workflow_success(wf_name: str, result) -> None:
    """Print success output for a workflow."""
    console.print(f"[green]✓ Workflow '{wf_name}' completed successfully[/green]")
    console.print(f"  Run ID: {result.run_id}")
    if result.duration_seconds:
        console.print(f"  Duration: {result.duration_seconds:.2f}s")

    for step_name, step_result in result.step_results.items():
        if step_result.output.get("object_path"):
            console.print(f"  {step_name}: stored at {step_result.output['object_path']}")
        if step_result.output.get("quality"):
            quality = step_result.output["quality"]
            score = quality.get("overall_score", 0)
            tokens = quality.get("estimated_tokens", 0)
            if score >= 80:
                score_color = "green"
            elif score >= 60:
                score_color = "yellow"
            else:
                score_color = "red"
            console.print(
                f"    Quality: [{score_color}]{score:.1f}/100[/{score_color}] "
                f"| Tokens: {tokens:,} "
                f"| AI-ready: {quality.get('ai_readiness_score', 0):.1f}"
            )


@app.command("list")
def list_resources(
    resource_type: str = typer.Argument(
        "all", help="Resource type: agencies, assets, workflows, or all"
    ),
) -> None:
    """List registered resources."""
    try:
        registry = load_registry()

        if resource_type in ("all", "agencies"):
            table = Table(title="Agencies")
            table.add_column("Name", style="cyan")
            table.add_column("Full Name")
            table.add_column("Labels")

            for name, agency in sorted(registry.agencies.items()):
                labels = ", ".join(f"{k}={v}" for k, v in agency.metadata.labels.items())
                table.add_row(name, agency.spec.full_name, labels or "-")

            console.print(table)
            console.print()

        if resource_type in ("all", "assets"):
            table = Table(title="Assets")
            table.add_column("Name", style="cyan")
            table.add_column("Agency")
            table.add_column("Type")
            table.add_column("Format")

            for name, asset in sorted(registry.assets.items()):
                table.add_row(
                    name,
                    asset.spec.agency_ref,
                    asset.spec.acquisition.type.value,
                    asset.spec.acquisition.format,
                )

            console.print(table)
            console.print()

        if resource_type in ("all", "workflows"):
            table = Table(title="Workflows")
            table.add_column("Name", style="cyan")
            table.add_column("Asset")
            table.add_column("Steps")

            for name, workflow in sorted(registry.workflows.items()):
                steps = ", ".join(s.name for s in workflow.spec.steps)
                table.add_row(name, workflow.spec.asset_ref, steps)

            console.print(table)

    except RegistryError as e:
        console.print(f"[red]Registry error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def storage(
    action: str = typer.Argument("list", help="Action: list, versions"),
    agency: str | None = typer.Option(None, "--agency", "-a", help="Filter by agency"),
    asset: str | None = typer.Option(None, "--asset", "-s", help="Asset name (for versions)"),
) -> None:
    """Interact with MinIO storage."""
    try:
        minio = MinioStorage()

        if action == "list":
            assets = minio.list_assets(agency)
            if not assets:
                console.print("[yellow]No assets found in storage[/yellow]")
                return

            table = Table(title="Stored Assets")
            table.add_column("Agency", style="cyan")
            table.add_column("Asset")

            for asset_path in assets:
                parts = asset_path.split("/")
                table.add_row(parts[0], parts[1] if len(parts) > 1 else "-")

            console.print(table)

        elif action == "versions":
            if not agency or not asset:
                console.print("[red]--agency and --asset required for versions action[/red]")
                raise typer.Exit(1)

            versions = minio.list_versions(agency, asset)
            if not versions:
                console.print(f"[yellow]No versions found for {agency}/{asset}[/yellow]")
                return

            table = Table(title=f"Versions: {agency}/{asset}")
            table.add_column("Timestamp", style="cyan")

            for version in versions:
                table.add_row(version)

            console.print(table)

    except StorageError as e:
        console.print(f"[red]Storage error: {e}[/red]")
        raise typer.Exit(1)


@app.command()
def compile(
    workflow: str = typer.Argument(..., help="Workflow name to compile"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Compile a workflow into an execution plan."""
    try:
        registry = load_registry()
        compiler = Compiler(registry)

        plan = compiler.compile(workflow)

        if output_json:
            console.print(json.dumps(plan.to_dict(), indent=2))
        else:
            console.print(f"[bold]Execution Plan: {plan.workflow_name}[/bold]")
            console.print(f"  Asset: {plan.asset.metadata.name}")
            console.print(f"  Agency: {plan.agency.metadata.name}")
            console.print(f"  Compiled at: {plan.compiled_at.isoformat()}")
            console.print(f"\n[bold]Steps ({len(plan.steps)}):[/bold]")
            for step in plan.steps:
                deps = f" (depends on: {', '.join(step.dependencies)})" if step.dependencies else ""
                console.print(f"  - {step.name} ({step.type}){deps}")
            console.print(f"\n[bold]Execution order:[/bold] {' -> '.join(plan.execution_order)}")

            if plan.validation:
                if plan.validation.valid:
                    console.print(f"\n[green]✓ Validation passed[/green]")
                else:
                    console.print(f"\n[red]✗ Validation failed:[/red]")
                    for error in plan.validation.errors:
                        console.print(f"  - {error}")

    except RegistryError as e:
        console.print(f"[red]Registry error: {e}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Compilation error: {e}[/red]")
        raise typer.Exit(1)


@app.command("run-step")
def run_step(
    step_type: str = typer.Argument(
        ..., help="Step type to run: acquire, parse, or all"
    ),
    workflow: str | None = typer.Option(
        None, "--workflow", "-w", help="Run only for specific workflow"
    ),
    dry_run: bool = typer.Option(False, "--dry-run", "-n", help="Validate without executing"),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output result as JSON"),
) -> None:
    """Run a specific step type across all workflows.

    This command allows running acquisition or parse steps independently:

    - acquire: Run acquisition step for all workflows
    - parse: Run parse step for all workflows (uses latest file from landing-zone)
    - all: Run all steps for all workflows (same as 'run --all')

    Examples:
        uv run pipeline run-step acquire
        uv run pipeline run-step parse
        uv run pipeline run-step all
        uv run pipeline run-step parse --workflow uscis-daca-pipeline
    """
    from storage.naming import LANDING_ZONE

    if step_type not in ("acquire", "parse", "all"):
        console.print(f"[red]Invalid step type: {step_type}. Use 'acquire', 'parse', or 'all'[/red]")
        raise typer.Exit(1)

    try:
        registry = load_registry()
        storage = MinioStorage()
        compiler = Compiler(registry)
        executor = SequentialExecutor(console if not output_json else None)

        # Determine which workflows to process
        if workflow:
            if workflow not in registry.workflows:
                console.print(f"[red]Workflow not found: {workflow}[/red]")
                raise typer.Exit(1)
            workflows_to_run = [workflow]
        else:
            workflows_to_run = list(registry.workflows.keys())

        # Filter workflows that have the requested step
        if step_type != "all":
            filtered_workflows = []
            for wf_name in workflows_to_run:
                wf = registry.workflows[wf_name]
                step_types = [s.type for s in wf.spec.steps]
                if step_type == "acquire" and "acquisition" in step_types:
                    filtered_workflows.append(wf_name)
                elif step_type == "parse" and "parse" in step_types:
                    filtered_workflows.append(wf_name)
            workflows_to_run = filtered_workflows

        if not workflows_to_run:
            console.print(f"[yellow]No workflows found with step type: {step_type}[/yellow]")
            raise typer.Exit(0)

        if not output_json:
            console.print(f"[bold]Running '{step_type}' step for {len(workflows_to_run)} workflow(s)...[/bold]\n")

        all_results = []
        failed_count = 0
        skipped_count = 0

        for wf_name in workflows_to_run:
            if not output_json:
                console.print(f"[bold cyan]{wf_name}[/bold cyan]")

            try:
                # Compile workflow to get execution plan
                plan = compiler.compile(wf_name)

                # Create execution context
                context = ExecutionContext(plan=plan, storage=storage)

                if step_type == "all":
                    # Run full workflow
                    result = executor.execute(plan, context, dry_run=dry_run)
                    all_results.append(result)
                else:
                    # Run specific step
                    # For parse step, we need to populate acquisition output from storage
                    if step_type == "parse":
                        # Find latest file in landing zone
                        agency_name = plan.agency.metadata.name
                        asset_name = plan.asset.metadata.name

                        latest_info = _get_latest_landing_zone_file(
                            storage, agency_name, asset_name
                        )

                        if latest_info is None:
                            if not output_json:
                                console.print(f"  [yellow]⚠ Skipped - no file in landing zone[/yellow]\n")
                            skipped_count += 1
                            continue

                        # Set acquisition output so parse step can use it
                        context.set_step_output("acquire", latest_info)

                    # Get the step name for the requested type
                    step_name = None
                    for step in plan.steps:
                        if (step_type == "acquire" and step.type == "acquisition") or \
                           (step_type == "parse" and step.type == "parse"):
                            step_name = step.name
                            break

                    if step_name is None:
                        if not output_json:
                            console.print(f"  [yellow]⚠ No {step_type} step found[/yellow]\n")
                        skipped_count += 1
                        continue

                    if dry_run:
                        if not output_json:
                            console.print(f"  [yellow]Dry run - would execute: {step_name}[/yellow]\n")
                        continue

                    # Execute the single step
                    step_result = executor.execute_step(step_name, plan, context)

                    if step_result.status.value == "success":
                        if not output_json:
                            console.print(f"  [green]✓ {step_name} completed[/green]")
                            if step_result.output.get("object_path"):
                                console.print(f"    Stored at: {step_result.output['object_path']}")
                            if step_result.output.get("quality"):
                                quality = step_result.output["quality"]
                                score = quality.get("overall_score", 0)
                                console.print(f"    Quality: {score:.1f}/100")
                    else:
                        failed_count += 1
                        if not output_json:
                            console.print(f"  [red]✗ {step_name} failed: {step_result.error}[/red]")

                    # Create a minimal result for JSON output
                    from runtime.executor import ExecutionResult
                    result = ExecutionResult(
                        plan_id=plan.plan_id,
                        workflow_name=plan.workflow_name,
                        run_id=context.run_id,
                        started_at=step_result.started_at,
                        completed_at=step_result.completed_at,
                    )
                    result.step_results[step_name] = step_result
                    result.status = step_result.status.value
                    all_results.append(result)

                if not output_json:
                    console.print()

            except Exception as e:
                failed_count += 1
                if not output_json:
                    console.print(f"  [red]✗ Failed: {e}[/red]\n")

        if output_json:
            if len(all_results) == 1:
                console.print(json.dumps(all_results[0].to_dict(), indent=2))
            else:
                console.print(json.dumps([r.to_dict() for r in all_results], indent=2))
        else:
            success_count = len(workflows_to_run) - failed_count - skipped_count
            console.print(f"[bold]Summary: {success_count} succeeded, {failed_count} failed, {skipped_count} skipped[/bold]")

        if failed_count > 0:
            raise typer.Exit(1)

    except RegistryError as e:
        console.print(f"[red]Registry error: {e}[/red]")
        raise typer.Exit(1)
    except StorageError as e:
        console.print(f"[red]Storage error: {e}[/red]")
        raise typer.Exit(1)


def _get_latest_landing_zone_file(
    storage: MinioStorage, agency: str, asset: str
) -> dict | None:
    """Get info about the latest file in landing zone for an asset.

    Args:
        storage: MinIO storage client
        agency: Agency name
        asset: Asset name

    Returns:
        Dictionary with acquisition-like output, or None if not found
    """
    from storage.naming import LANDING_ZONE

    try:
        prefix = f"{LANDING_ZONE}/{agency}/{asset}/"
        objects = list(storage.client.list_objects(storage.bucket, prefix=prefix, recursive=True))

        if not objects:
            return None

        # Find the most recent object
        latest = max(objects, key=lambda x: x.last_modified)
        object_path = latest.object_name
        object_size = latest.size

        # Get object metadata
        metadata = {}
        try:
            stat = storage.client.stat_object(storage.bucket, object_path)
            metadata = stat.metadata or {}
        except Exception:
            pass

        # Extract format from filename
        filename = object_path.split("/")[-1]
        file_format = filename.split(".")[-1] if "." in filename else ""

        # Build acquisition-like output
        return {
            "object_path": object_path,
            "bytes_stored": object_size,
            "source_url": metadata.get("x-amz-meta-source_url", ""),
            "format": file_format,
            "checksum": metadata.get("x-amz-meta-checksum", ""),
            "connector_type": metadata.get("x-amz-meta-connector_type", ""),
            "zone": LANDING_ZONE,
        }

    except Exception:
        return None


@app.command()
def report(
    output: str = typer.Option(
        "./pipeline-report.html",
        "--output",
        "-o",
        help="Output HTML file path",
    ),
    open_browser: bool = typer.Option(
        False,
        "--open",
        "-O",
        help="Open report in browser after generation",
    ),
) -> None:
    """Generate an HTML status report with quality metrics."""
    try:
        registry = load_registry()
        storage = MinioStorage()

        console.print("[bold]Generating pipeline status report...[/bold]")

        output_path = Path(output)
        html = generate_html_report(registry, storage, output_path)

        console.print(f"[green]✓ Report generated: {output_path.absolute()}[/green]")

        # Show summary
        from reports.html_report import collect_workflow_reports
        reports = collect_workflow_reports(registry, storage)

        successful = sum(1 for r in reports if r.overall_status == "success")
        with_quality = [r for r in reports if r.quality_score is not None]
        avg_quality = sum(r.quality_score for r in with_quality) / len(with_quality) if with_quality else 0

        console.print(f"\n[bold]Summary:[/bold]")
        console.print(f"  Workflows: {len(reports)}")
        console.print(f"  Successful: {successful}")
        console.print(f"  With quality metrics: {len(with_quality)}")
        if with_quality:
            console.print(f"  Average quality score: {avg_quality:.1f}/100")

        if open_browser:
            import webbrowser
            webbrowser.open(f"file://{output_path.absolute()}")

    except RegistryError as e:
        console.print(f"[red]Registry error: {e}[/red]")
        raise typer.Exit(1)
    except StorageError as e:
        console.print(f"[red]Storage error: {e}[/red]")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]Error generating report: {e}[/red]")
        raise typer.Exit(1)


# =============================================================================
# MDA Migration Commands
# =============================================================================


@app.command()
def migrate(
    workflow: str | None = typer.Argument(
        None, help="Workflow name to migrate (omit for --all)"
    ),
    all_workflows: bool = typer.Option(
        False, "--all", "-a", help="Migrate all workflows"
    ),
    output_dir: str = typer.Option(
        None, "--output", "-o", help="Output directory (default: manifests/mda)"
    ),
) -> None:
    """Migrate pipeline/v1 manifests to standard/1.0 format.

    Converts existing manifests to MDA standard/1.0 format. The original
    manifests are preserved; new files are written to manifests/mda/.

    Examples:
        uv run pipeline migrate my-workflow
        uv run pipeline migrate --all
        uv run pipeline migrate --all --output ./mda-manifests
    """
    from mda.migration.manifest_converter import convert_to_yaml

    try:
        registry = load_registry()

        if not workflow and not all_workflows:
            console.print("[red]Error: Specify a workflow name or use --all[/red]")
            raise typer.Exit(1)

        if all_workflows:
            workflows_to_migrate = list(registry.workflows.keys())
        else:
            if workflow not in registry.workflows:
                console.print(f"[red]Workflow not found: {workflow}[/red]")
                raise typer.Exit(1)
            workflows_to_migrate = [workflow]

        # Determine output directory
        manifests_path = get_manifests_path()
        out_path = Path(output_dir) if output_dir else manifests_path / "mda"
        out_path.mkdir(parents=True, exist_ok=True)

        console.print(f"[bold]Migrating {len(workflows_to_migrate)} workflow(s) to {out_path}...[/bold]\n")

        migrated = 0
        failed = 0

        for wf_name in workflows_to_migrate:
            try:
                wf = registry.workflows[wf_name]
                asset = registry.get_workflow_asset(wf)
                agency = registry.get_workflow_agency(wf)

                yaml_content = convert_to_yaml(agency, asset, wf)

                # Write to output directory
                output_file = out_path / f"{wf_name}.yaml"
                output_file.write_text(yaml_content)

                console.print(f"  [green]✓ {wf_name}[/green] -> {output_file}")
                migrated += 1

            except Exception as e:
                console.print(f"  [red]✗ {wf_name}: {e}[/red]")
                failed += 1

        console.print(f"\n[bold]Migration complete: {migrated} succeeded, {failed} failed[/bold]")

        if failed > 0:
            raise typer.Exit(1)

    except RegistryError as e:
        console.print(f"[red]Registry error: {e}[/red]")
        raise typer.Exit(1)


# =============================================================================
# Database Commands
# =============================================================================


def get_db_engine():
    """Get database engine from DATABASE_URL or fallback to DATABASE_PATH."""
    from db.database import get_engine

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        # Use DATABASE_URL directly (PostgreSQL or other)
        return get_engine()
    # Legacy fallback: SQLite from DATABASE_PATH
    db_path = os.getenv("DATABASE_PATH", "./pipeline.db")
    return get_engine(db_path)


@db_app.command("sync")
def db_sync(
    db_path: str | None = typer.Option(
        None, "--db-path", help="Path to SQLite database file (legacy, prefer DATABASE_URL env var)"
    ),
) -> None:
    """Sync manifests to database."""
    from db.database import get_engine
    from db.sync import ManifestSync

    manifests_path = get_manifests_path()
    if not manifests_path.exists():
        console.print(f"[red]Manifests directory not found: {manifests_path}[/red]")
        raise typer.Exit(1)

    # Get or create engine
    if db_path:
        engine = get_engine(db_path)
    else:
        engine = get_db_engine()

    try:
        sync = ManifestSync(manifests_path, engine)
        console.print(f"[bold]Syncing manifests to database...[/bold]")
        report = sync.sync()

        # Display results
        console.print(f"\n[bold]Sync Results:[/bold]")
        console.print(
            f"  Agencies: [green]{report.agencies_synced} synced[/green], "
            f"[red]{report.agencies_errors} errors[/red]"
        )
        console.print(
            f"  Assets: [green]{report.assets_synced} synced[/green], "
            f"[red]{report.assets_errors} errors[/red]"
        )
        console.print(
            f"  Workflows: [green]{report.workflows_synced} synced[/green], "
            f"[red]{report.workflows_errors} errors[/red]"
        )

        if report.errors:
            console.print(f"\n[red]Errors:[/red]")
            for error in report.errors:
                console.print(f"  [{error.entity_type}] {error.entity_name}: {error.error}")

        if report.success:
            console.print(f"\n[green]Sync completed successfully![/green]")
        else:
            console.print(f"\n[yellow]Sync completed with errors.[/yellow]")
            raise typer.Exit(1)

    except Exception as e:
        console.print(f"[red]Sync error: {e}[/red]")
        raise typer.Exit(1)


@db_app.command("plan")
def db_plan(
    workflow: str | None = typer.Argument(
        None, help="Workflow name to compile (omit for --all)"
    ),
    all_workflows: bool = typer.Option(False, "--all", "-a", help="Compile all workflows"),
    db_path: str | None = typer.Option(
        None, "--db-path", help="Path to SQLite database file (legacy, prefer DATABASE_URL env var)"
    ),
    output_json: bool = typer.Option(False, "--json", "-j", help="Output as JSON"),
) -> None:
    """Generate execution plan from database."""
    from db.database import get_engine
    from db.db_registry import DbRegistry

    if not workflow and not all_workflows:
        console.print("[red]Error: Specify a workflow name or use --all[/red]")
        raise typer.Exit(1)

    # Get or create engine
    if db_path:
        engine = get_engine(db_path)
    else:
        engine = get_db_engine()

    try:
        # Load registry from database
        registry = DbRegistry(engine)
        registry.load()

        # Compile using existing compiler
        compiler = Compiler(registry)

        # Determine which workflows to compile
        if all_workflows:
            workflows_to_compile = list(registry.workflows.keys())
            if not output_json:
                console.print(
                    f"[bold]Compiling all {len(workflows_to_compile)} workflows...[/bold]\n"
                )
        else:
            workflows_to_compile = [workflow]

        all_plans = []
        failed_count = 0

        for wf_name in workflows_to_compile:
            try:
                plan = compiler.compile(wf_name)
                all_plans.append(plan)

                if output_json:
                    continue  # Collect all results first

                if all_workflows:
                    console.print(f"[bold cyan]{wf_name}[/bold cyan]")

                console.print(f"[bold]Execution Plan: {plan.workflow_name}[/bold]")
                console.print(f"  Asset: {plan.asset.metadata.name}")
                console.print(f"  Agency: {plan.agency.metadata.name}")
                console.print(f"  Compiled at: {plan.compiled_at.isoformat()}")
                console.print(f"  [dim](from database)[/dim]")
                console.print(f"\n[bold]Steps ({len(plan.steps)}):[/bold]")
                for step in plan.steps:
                    deps = (
                        f" (depends on: {', '.join(step.dependencies)})"
                        if step.dependencies
                        else ""
                    )
                    console.print(f"  - {step.name} ({step.type}){deps}")
                console.print(
                    f"\n[bold]Execution order:[/bold] {' -> '.join(plan.execution_order)}"
                )

                if plan.validation:
                    if plan.validation.valid:
                        console.print(f"\n[green]✓ Validation passed[/green]")
                    else:
                        console.print(f"\n[red]✗ Validation failed:[/red]")
                        for error in plan.validation.errors:
                            console.print(f"  - {error}")

                if all_workflows:
                    console.print()

            except Exception as e:
                failed_count += 1
                if not output_json:
                    console.print(f"[red]✗ {wf_name}: {e}[/red]\n")

        if output_json:
            if len(all_plans) == 1:
                console.print(json.dumps(all_plans[0].to_dict(), indent=2))
            else:
                console.print(json.dumps([p.to_dict() for p in all_plans], indent=2))

        if all_workflows and not output_json:
            success_count = len(workflows_to_compile) - failed_count
            console.print(
                f"[bold]Summary: {success_count}/{len(workflows_to_compile)} compiled successfully[/bold]"
            )

        if failed_count > 0:
            raise typer.Exit(1)

    except RegistryError as e:
        console.print(f"[red]Registry error: {e}[/red]")
        raise typer.Exit(1)


@db_app.command("list")
def db_list(
    resource_type: str = typer.Argument(
        "all", help="Resource type: agencies, assets, workflows, or all"
    ),
    db_path: str | None = typer.Option(
        None, "--db-path", help="Path to SQLite database file (legacy, prefer DATABASE_URL env var)"
    ),
) -> None:
    """List entities from database."""
    from db.database import get_engine, get_session
    from db.repository import AgencyRepository, AssetRepository, WorkflowRepository

    # Get or create engine
    if db_path:
        engine = get_engine(db_path)
    else:
        engine = get_db_engine()

    try:
        with get_session(engine) as session:
            if resource_type in ("all", "agencies"):
                agency_repo = AgencyRepository(session)
                agencies = agency_repo.list_all()

                table = Table(title="Agencies (from database)")
                table.add_column("ID", style="dim")
                table.add_column("Name", style="cyan")
                table.add_column("Full Name")
                table.add_column("Updated")

                for agency in agencies:
                    table.add_row(
                        str(agency.id),
                        agency.name,
                        agency.full_name,
                        agency.updated_at.strftime("%Y-%m-%d %H:%M"),
                    )

                console.print(table)
                console.print()

            if resource_type in ("all", "assets"):
                asset_repo = AssetRepository(session)
                agency_repo = AgencyRepository(session)
                assets = asset_repo.list_all()

                # Build agency ID to name map
                agency_map = {a.id: a.name for a in agency_repo.list_all()}

                table = Table(title="Assets (from database)")
                table.add_column("ID", style="dim")
                table.add_column("Name", style="cyan")
                table.add_column("Agency")
                table.add_column("Format")
                table.add_column("Updated")

                for asset in assets:
                    acq_format = asset.acquisition_config.get("format", "-")
                    agency_name = agency_map.get(asset.agency_id, "-")
                    table.add_row(
                        str(asset.id),
                        asset.name,
                        agency_name,
                        acq_format,
                        asset.updated_at.strftime("%Y-%m-%d %H:%M"),
                    )

                console.print(table)
                console.print()

            if resource_type in ("all", "workflows"):
                workflow_repo = WorkflowRepository(session)
                asset_repo = AssetRepository(session)
                workflows = workflow_repo.list_all()

                # Build asset ID to name map
                asset_map = {a.id: a.name for a in asset_repo.list_all()}

                table = Table(title="Workflows (from database)")
                table.add_column("ID", style="dim")
                table.add_column("Name", style="cyan")
                table.add_column("Asset")
                table.add_column("Steps")
                table.add_column("Updated")

                for wf in workflows:
                    asset_name = asset_map.get(wf.asset_id, "-")
                    step_names = ", ".join(s.get("name", "?") for s in wf.steps)
                    table.add_row(
                        str(wf.id),
                        wf.name,
                        asset_name,
                        step_names,
                        wf.updated_at.strftime("%Y-%m-%d %H:%M"),
                    )

                console.print(table)

    except Exception as e:
        console.print(f"[red]Database error: {e}[/red]")
        raise typer.Exit(1)


@db_app.command("status")
def db_status(
    last: int = typer.Option(20, "--last", "-n", help="Number of recent entries to show"),
    db_path: str | None = typer.Option(
        None, "--db-path", help="Path to SQLite database file (legacy, prefer DATABASE_URL env var)"
    ),
) -> None:
    """Show sync status and history."""
    from db.database import get_engine, get_session
    from db.repository import SyncLogRepository

    # Get or create engine
    if db_path:
        engine = get_engine(db_path)
    else:
        engine = get_db_engine()

    try:
        with get_session(engine) as session:
            sync_log_repo = SyncLogRepository(session)
            logs = sync_log_repo.get_recent(last)

            if not logs:
                console.print("[yellow]No sync history found.[/yellow]")
                return

            table = Table(title="Sync History")
            table.add_column("Time", style="dim")
            table.add_column("Type")
            table.add_column("Name", style="cyan")
            table.add_column("Status")
            table.add_column("Error")

            for log in logs:
                status_style = "green" if log.status == "success" else "red"
                error_msg = log.error_message[:50] + "..." if log.error_message and len(log.error_message) > 50 else (log.error_message or "-")
                table.add_row(
                    log.synced_at.strftime("%Y-%m-%d %H:%M:%S"),
                    log.entity_type,
                    log.entity_name,
                    f"[{status_style}]{log.status}[/{status_style}]",
                    error_msg,
                )

            console.print(table)

            # Show summary
            errors = sync_log_repo.get_errors(100)
            if errors:
                console.print(f"\n[yellow]Recent errors: {len(errors)}[/yellow]")

    except Exception as e:
        console.print(f"[red]Database error: {e}[/red]")
        raise typer.Exit(1)


# =============================================================================
# Log Commands
# =============================================================================


@logs_app.command("list-steps")
def logs_list_steps() -> None:
    """List all distinct step values recorded in the logs.

    Examples:
        uv run pipeline logs list-steps
    """
    from sqlalchemy import select, func
    from db.database import get_session
    from db.models import PipelineLogModel

    engine = get_db_engine()

    try:
        with get_session(engine) as session:
            stmt = (
                select(
                    PipelineLogModel.step,
                    func.count(PipelineLogModel.id).label("count"),
                    func.max(PipelineLogModel.timestamp).label("last_seen"),
                )
                .where(PipelineLogModel.step.isnot(None))
                .group_by(PipelineLogModel.step)
                .order_by(func.count(PipelineLogModel.id).desc())
            )
            rows = session.execute(stmt).all()

            if not rows:
                console.print("[yellow]No step values found in logs yet.[/yellow]")
                console.print("[dim]Steps are recorded when you run workflows or sync the database.[/dim]")
                return

            table = Table(title="Log Steps")
            table.add_column("Step", style="cyan")
            table.add_column("Log Count", justify="right")
            table.add_column("Last Seen", style="dim")

            for step_name, count, last_seen in rows:
                table.add_row(
                    step_name,
                    str(count),
                    last_seen.strftime("%Y-%m-%d %H:%M:%S") if last_seen else "-",
                )

            console.print(table)

    except Exception as e:
        console.print(f"[red]Error querying logs: {e}[/red]")
        raise typer.Exit(1)


@logs_app.command("show")
def logs_show(
    last: int = typer.Option(50, "--last", "-n", help="Number of recent log entries"),
    level: str | None = typer.Option(None, "--level", "-l", help="Filter by level (INFO, WARNING, ERROR)"),
    run_id: str | None = typer.Option(None, "--run-id", "-r", help="Filter by run ID (UTID)"),
    workflow: str | None = typer.Option(None, "--workflow", "-w", help="Filter by workflow name"),
    step: str | None = typer.Option(None, "--step", "-s", help="Filter by step name"),
    search: str | None = typer.Option(None, "--search", "-q", help="Search in log messages"),
) -> None:
    """Show recent pipeline log entries from the database.

    Examples:
        uv run pipeline logs show
        uv run pipeline logs show --level ERROR
        uv run pipeline logs show --run-id utid-a1b2c3d4e5f6
        uv run pipeline logs show --workflow uscis-forms-pipeline --last 20
        uv run pipeline logs show --search "timeout"
    """
    from sqlalchemy import select, desc
    from db.database import get_session
    from db.models import PipelineLogModel

    engine = get_db_engine()

    try:
        with get_session(engine) as session:
            stmt = select(PipelineLogModel)

            if level:
                stmt = stmt.where(PipelineLogModel.level == level.upper())
            if run_id:
                stmt = stmt.where(PipelineLogModel.run_id == run_id)
            if workflow:
                stmt = stmt.where(PipelineLogModel.workflow == workflow)
            if step:
                stmt = stmt.where(PipelineLogModel.step == step)
            if search:
                stmt = stmt.where(PipelineLogModel.message.ilike(f"%{search}%"))

            stmt = stmt.order_by(desc(PipelineLogModel.timestamp)).limit(last)
            logs = list(session.execute(stmt).scalars().all())

            if not logs:
                console.print("[yellow]No log entries found.[/yellow]")
                return

            # Display oldest-first for readability
            logs.reverse()

            table = Table(title=f"Pipeline Logs (last {len(logs)})")
            table.add_column("Timestamp", style="dim", width=19)
            table.add_column("Level", width=7)
            table.add_column("Run ID", style="magenta", width=18)
            table.add_column("Workflow", style="cyan", width=20)
            table.add_column("Step", width=12)
            table.add_column("Message", no_wrap=False)

            level_colors = {"INFO": "blue", "WARNING": "yellow", "ERROR": "red"}

            for log in logs:
                lvl_color = level_colors.get(log.level, "white")
                table.add_row(
                    log.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    f"[{lvl_color}]{log.level}[/{lvl_color}]",
                    log.run_id or "-",
                    log.workflow or "-",
                    log.step or "-",
                    log.message[:120],
                )

            console.print(table)

    except Exception as e:
        console.print(f"[red]Error querying logs: {e}[/red]")
        raise typer.Exit(1)


@logs_app.command("tail")
def logs_tail(
    follow_interval: float = typer.Option(2.0, "--interval", "-i", help="Poll interval in seconds"),
    level: str | None = typer.Option(None, "--level", "-l", help="Filter by level"),
    run_id: str | None = typer.Option(None, "--run-id", "-r", help="Filter by run ID (UTID)"),
    workflow: str | None = typer.Option(None, "--workflow", "-w", help="Filter by workflow"),
) -> None:
    """Tail pipeline logs in real time (polls the database).

    Examples:
        uv run pipeline logs tail
        uv run pipeline logs tail --level ERROR --interval 1
        uv run pipeline logs tail --run-id utid-a1b2c3d4e5f6
    """
    import time
    from sqlalchemy import select, desc
    from db.database import get_session
    from db.models import PipelineLogModel

    engine = get_db_engine()
    last_id = 0

    console.print("[dim]Tailing pipeline logs (Ctrl+C to stop)...[/dim]\n")

    try:
        while True:
            with get_session(engine) as session:
                stmt = select(PipelineLogModel).where(PipelineLogModel.id > last_id)

                if level:
                    stmt = stmt.where(PipelineLogModel.level == level.upper())
                if run_id:
                    stmt = stmt.where(PipelineLogModel.run_id == run_id)
                if workflow:
                    stmt = stmt.where(PipelineLogModel.workflow == workflow)

                stmt = stmt.order_by(PipelineLogModel.id).limit(100)
                logs = list(session.execute(stmt).scalars().all())

                for log in logs:
                    last_id = log.id
                    lvl = log.level
                    color = {"INFO": "cyan", "WARNING": "yellow", "ERROR": "red"}.get(lvl, "white")
                    rid = f" {log.run_id}" if log.run_id else ""
                    wf = f" [{log.workflow}]" if log.workflow else ""
                    st = f" ({log.step})" if log.step else ""
                    console.print(
                        f"[dim]{log.timestamp.strftime('%H:%M:%S')}[/dim] "
                        f"[{color}]{lvl:7s}[/{color}][magenta]{rid}[/magenta]{wf}{st} {log.message}"
                    )

            time.sleep(follow_interval)

    except KeyboardInterrupt:
        console.print("\n[dim]Stopped.[/dim]")


@logs_app.command("clear")
def logs_clear(
    before_days: int | None = typer.Option(None, "--before-days", help="Clear logs older than N days"),
    confirm: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation"),
) -> None:
    """Clear log entries from the database.

    Examples:
        uv run pipeline logs clear --before-days 30
        uv run pipeline logs clear --yes
    """
    from sqlalchemy import delete, func, select
    from db.database import get_session
    from db.models import PipelineLogModel

    engine = get_db_engine()

    try:
        with get_session(engine) as session:
            count_stmt = select(func.count(PipelineLogModel.id))
            if before_days is not None:
                from datetime import timedelta
                cutoff = datetime.now(timezone.utc) - timedelta(days=before_days)
                count_stmt = count_stmt.where(PipelineLogModel.timestamp < cutoff)

            total = session.execute(count_stmt).scalar() or 0

            if total == 0:
                console.print("[yellow]No matching log entries to clear.[/yellow]")
                return

            scope = f"older than {before_days} days" if before_days else "all"
            if not confirm:
                console.print(f"About to delete [bold]{total}[/bold] log entries ({scope}).")
                response = typer.confirm("Continue?")
                if not response:
                    raise typer.Abort()

            del_stmt = delete(PipelineLogModel)
            if before_days is not None:
                from datetime import timedelta
                cutoff = datetime.now(timezone.utc) - timedelta(days=before_days)
                del_stmt = del_stmt.where(PipelineLogModel.timestamp < cutoff)

            session.execute(del_stmt)
            console.print(f"[green]✓ Deleted {total} log entries.[/green]")

    except typer.Abort:
        console.print("[dim]Cancelled.[/dim]")
    except Exception as e:
        console.print(f"[red]Error clearing logs: {e}[/red]")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
