import logging
import json
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict
import traceback

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@dataclass
class DebugStep:
    """Represents a step in the RAG flow"""
    name: str
    status: str  # 'started', 'completed', 'failed'
    start_time: datetime
    end_time: Optional[datetime] = None
    duration_ms: Optional[float] = None
    details: Optional[Dict[str, Any]] = None
    error: Optional[str] = None

class DebugService:
    """Service for debugging and monitoring RAG flow"""
    
    def __init__(self):
        self.steps: List[DebugStep] = []
        self.current_step: Optional[DebugStep] = None
    
    def start_step(self, name: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Start a new step in the RAG flow"""
        if self.current_step:
            self.end_step()
        
        self.current_step = DebugStep(
            name=name,
            status='started',
            start_time=datetime.now(),
            details=details
        )
        self.steps.append(self.current_step)
        
        logger.debug(f"Started step: {name}")
        if details:
            logger.debug(f"Step details: {json.dumps(details, indent=2)}")
    
    def end_step(self, error: Optional[Exception] = None) -> None:
        """End the current step"""
        if not self.current_step:
            return
        
        end_time = datetime.now()
        duration = (end_time - self.current_step.start_time).total_seconds() * 1000
        
        self.current_step.end_time = end_time
        self.current_step.duration_ms = duration
        self.current_step.status = 'failed' if error else 'completed'
        
        if error:
            self.current_step.error = str(error)
            logger.error(f"Step {self.current_step.name} failed: {str(error)}")
            logger.error(traceback.format_exc())
        else:
            logger.debug(f"Completed step: {self.current_step.name} in {duration:.2f}ms")
        
        self.current_step = None
    
    def add_step_details(self, details: Dict[str, Any]) -> None:
        """Add details to the current step"""
        if not self.current_step:
            return
        
        if not self.current_step.details:
            self.current_step.details = {}
        
        self.current_step.details.update(details)
        logger.debug(f"Updated step details: {json.dumps(details, indent=2)}")
    
    def get_flow_summary(self) -> Dict[str, Any]:
        """Get a summary of the RAG flow"""
        return {
            'total_steps': len(self.steps),
            'completed_steps': sum(1 for s in self.steps if s.status == 'completed'),
            'failed_steps': sum(1 for s in self.steps if s.status == 'failed'),
            'total_duration_ms': sum(s.duration_ms or 0 for s in self.steps),
            'steps': [asdict(s) for s in self.steps]
        }
    
    def log_flow_summary(self) -> None:
        """Log a summary of the RAG flow"""
        summary = self.get_flow_summary()
        logger.info("RAG Flow Summary:")
        logger.info(f"Total steps: {summary['total_steps']}")
        logger.info(f"Completed steps: {summary['completed_steps']}")
        logger.info(f"Failed steps: {summary['failed_steps']}")
        logger.info(f"Total duration: {summary['total_duration_ms']:.2f}ms")
        
        for step in summary['steps']:
            status_icon = "✅" if step['status'] == 'completed' else "❌" if step['status'] == 'failed' else "⏳"
            logger.info(f"{status_icon} {step['name']} ({step['duration_ms']:.2f}ms)")
            if step.get('error'):
                logger.error(f"Error: {step['error']}")
    
    def clear(self) -> None:
        """Clear all steps and reset the debug service"""
        self.steps = []
        self.current_step = None 