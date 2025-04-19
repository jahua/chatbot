from typing import Dict, Any, List, Optional
from datetime import datetime
import uuid

class DebugService:
    def __init__(self):
        """Initialize the DebugService with default values."""
        self.flow_id: str = str(uuid.uuid4())
        self.steps: List[Dict[str, Any]] = []
        self.start_time: datetime = datetime.now()
        self.end_time: Optional[datetime] = None
        self.error: Optional[str] = None

    def start_flow(self) -> None:
        """Start a new flow with a new ID and reset all attributes."""
        self.flow_id = str(uuid.uuid4())
        self.steps = []
        self.start_time = datetime.now()
        self.end_time = None
        self.error = None

    def add_step(self, step_name: str, details: Dict[str, Any]) -> None:
        """Add a step to the current flow with timestamp."""
        self.steps.append({
            "name": step_name,
            "timestamp": datetime.now(),
            "details": details
        })

    def end_flow(self, error: Optional[str] = None) -> None:
        """End the current flow and optionally record an error."""
        self.end_time = datetime.now()
        self.error = error

    def get_flow_info(self) -> Dict[str, Any]:
        """Get information about the current flow execution."""
        return {
            "flow_id": self.flow_id,
            "steps": self.steps,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": (self.end_time - self.start_time).total_seconds() if self.end_time else None,
            "error": self.error
        } 