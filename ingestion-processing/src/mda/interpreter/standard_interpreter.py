"""Standard Interpreter v1 — the universal execution loop.

Adapted from model_D/mda/provider/interpreter/v1/standard_interpreter.py.

This interpreter works with any manifest schema via the parser interface,
and resolves capabilities via the DefaultResolver. It uses the hybrid
ExecutionContext that carries both Model_D traceability fields and the
existing pipeline's storage and plan references.
"""

from typing import Any

from mda.constants import STATUS_SUCCESS, STATUS_FAILED
from mda.interpreter.interface import InterpreterInterface
from mda.resolver.default_resolver import DefaultResolver
from runtime.context import ExecutionContext


class StandardInterpreter(InterpreterInterface):
    """Universal interpreter for all providers.

    Execution flow:
    1. Receive parsed manifest (parser) and execution context
    2. Resolve each step's component_path to a capability class via DefaultResolver
    3. Instantiate capability with context + params
    4. Execute capability and store result in context
    5. Return aggregated results
    """

    RESOLVER_VERSION = "v1"

    def __init__(self, master_utid: str, manifest_urn: str) -> None:
        """Initialize interpreter.

        Note: Unlike Model_D's original, this interpreter does NOT load the manifest
        itself. Instead, the caller (execute_via_interpreter in SequentialExecutor)
        provides the parser and context directly via set_parser_and_context().
        This avoids duplicating the Registry + Compiler logic.

        Args:
            master_utid: Universal Trace ID.
            manifest_urn: Manifest URN being executed.
        """
        self.master_utid = master_utid
        self.manifest_urn = manifest_urn
        self._parser = None
        self._context: ExecutionContext | None = None
        self._resolver: DefaultResolver | None = None

    def set_parser_and_context(
        self,
        parser: Any,
        context: ExecutionContext,
        provider_config: dict | None = None,
    ) -> None:
        """Inject the parser and context from the caller.

        This is the hybrid integration point: the existing pipeline compiles
        the execution plan and creates the context, then hands them to the
        interpreter for Model_D-style execution.

        Args:
            parser: A ParserInterface implementation (e.g., LegacyPipelineParser).
            context: The hybrid ExecutionContext (with plan, storage, UTID).
            provider_config: Optional provider plugin config for resolver mappings.
        """
        self._parser = parser
        self._context = context

        # Set up resolver
        provider = parser.get_provider()
        engine = parser.get_engine()
        self._resolver = DefaultResolver(
            provider=provider,
            engine=engine,
            provider_config=provider_config,
        )

    def execute(self) -> dict[str, Any]:
        """The Universal Execution Loop.

        Returns:
            Result dict with status, utid, manifest_id, steps_executed, results.
        """
        if self._parser is None or self._context is None:
            raise RuntimeError(
                "StandardInterpreter requires set_parser_and_context() before execute(). "
                "The parser and context must be injected by the caller."
            )

        steps = self._parser.get_steps()

        for step in steps:
            step_name = self._parser.get_step_name(step)
            component_path = self._parser.get_step_component_path(step)
            params = self._parser.get_step_component_params(step)

            try:
                # Resolve component path to capability class
                CapabilityClass = self._resolver.resolve(component_path)

                # Instantiate and execute (validation runs in __init__)
                capability = CapabilityClass(context=self._context, params=params)
                result = capability.execute()

                # Store result in context (shared data store)
                self._context.append_result(step_name, result)

            except Exception as e:
                return {
                    "status": STATUS_FAILED,
                    "utid": self.master_utid,
                    "manifest_id": self._parser.get_manifest_id(),
                    "failed_step": step_name,
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "steps_executed": len(self._context.get_results()),
                    "results": self._context.get_results(),
                }

        return {
            "status": STATUS_SUCCESS,
            "utid": self.master_utid,
            "manifest_id": self._parser.get_manifest_id(),
            "manifest_version": self._parser.get_manifest_version(),
            "steps_executed": len(self._context.get_results()),
            "results": self._context.get_results(),
        }
