# =============================================================================
# PIPELINE EXECUTOR
# =============================================================================

import pathlib as Path
from shutil import copytree
import sys

from data_pipeline.shared.run_context import RunContext
from data_pipeline.stages.validate_raw_data import apply_validation

def snapshot_raw(run_context: RunContext) -> None:
    """
    Copy entire raw source into run-scoped raw_snapshot directory.
    """
    
    source = run_context.source_raw_path
    destination = run_context.raw_snapshot_path
    
    if not source.exists():
        raise FileNotFoundError(f'Raw source path not found: {source}')
    
    copytree(source, destination, dirs_exist_ok= True)
    
def main() -> None:
    run_context = RunContext.create()
    run_context.initialize_directories()
    
    snapshot_raw(run_context)
    
    report = apply_validation(run_context)
    
    if report['errors'] or report['warnings']:
        sys.exit(1)
    
    sys.exit(0)

if __name__ == '__main__':
    main()

# =============================================================================
# END OF SCRIPT
# =============================================================================