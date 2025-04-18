import logging
import json
from typing import Dict, Any, List, Optional, Union
from datetime import datetime
from dataclasses import dataclass, asdict
import traceback
import uuid
import time

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
        self.session_id: Optional[str] = None
        self.message_id: Optional[str] = None
        self.flow_start_time: Optional[datetime] = None
    
    def start_flow(self, session_id: str, message_id: Optional[str] = None) -> str:
        """Start a new debug flow, clearing any existing steps and storing IDs."""
        self.clear()
        self.session_id = session_id
        
        # Generate a message_id if not provided
        if message_id is None:
            message_id = str(uuid.uuid4())
            
        self.message_id = message_id
        self.flow_start_time = datetime.now()
        logger.debug(f"Started new debug flow for session {session_id}, message {message_id}")
        
        return message_id
    
    def start_step(self, name: str, details: Optional[Union[Dict[str, Any], str]] = None) -> None:
        """Start a new step in the RAG flow"""
        if self.current_step:
            self.end_step()
        
        # Convert string details to a dictionary if needed
        if isinstance(details, str):
            details = {"description": details}
        
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
    
    def end_step(self, name: Optional[str] = None, success: bool = True, error: Optional[Any] = None, details: Optional[Dict[str, Any]] = None) -> None:
        """End a step by name or the current step if no name is provided.
        
        Args:
            name: Name of the step to end (finds most recent if multiple steps with same name)
            success: Whether the step was successful
            error: Error message or exception object if there was an error
            details: Additional details to add to the step
        """
        # Determine target step
        target_step = None
        if name:
            # Find most recent step with this name by searching in reverse
            for step in reversed(self.steps):
                if step.name == name:  # Access attribute directly, not using get()
                    target_step = step
                    break
            if not target_step:
                logger.warning(f"Attempted to end non-existent step: {name}")
                return
        elif self.current_step:
            target_step = self.current_step
        else:
            logger.warning("Attempted to end step, but no current step exists")
            return
            
        # Calculate step duration
        end_time = datetime.now()
        target_step.end_time = end_time
        duration = (end_time - target_step.start_time).total_seconds() * 1000
        target_step.duration_ms = duration
            
        # Set the status based on success or error
        if error:
            target_step.status = "failed"
            target_step.error = str(error)
        else:
            target_step.status = "completed"
        
        # Add any details if provided
        if details:
            # Convert string details to dict if needed
            if isinstance(target_step.details, str):
                target_step.details = {"description": target_step.details}
                
            if target_step.details is None:
                target_step.details = {}
                
            target_step.details.update(details)
            
        # Log the step end
        logger.debug(f"Ended step: {target_step.name} with status {target_step.status}")
        
        # Clear the current step if we're ending that one
        if target_step is self.current_step:
            self.current_step = None
    
    def add_step_details(self, details: Dict[str, Any]) -> None:
        """Add details to the current step"""
        if not self.current_step:
            return
        
        # Convert string details to dict if needed
        if isinstance(self.current_step.details, str):
            self.current_step.details = {"description": self.current_step.details}
        
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
        self.session_id = None
        self.message_id = None
        self.flow_start_time = None
    
    def get_message_id(self) -> Optional[str]:
        """Return the message ID for the current flow."""
        return self.message_id
    
    def get_debug_info_for_response(self) -> Dict[str, Any]:
        """Get debug information formatted for the response payload"""
        summary = self.get_flow_summary()
        
        # Create a clean debug info object
        debug_info = {
            'status': 'success' if summary['failed_steps'] == 0 else 'error',
            'total_steps': summary['total_steps'],
            'completed_steps': summary['completed_steps'],
            'failed_steps': summary['failed_steps'],
            'total_duration_ms': summary['total_duration_ms'],
            'steps': []
        }
        
        # Format each step for the response
        for step in summary['steps']:
            step_info = {
                'name': step['name'],
                'status': step['status'],
                'duration_ms': step['duration_ms'],
                'error': step.get('error')
            }
            
            # Include only essential details
            if step.get('details'):
                # Filter details to include only what's relevant for debugging
                filtered_details = {}
                for key, value in step['details'].items():
                    # Include SQL queries, result counts, visualization info
                    if key in ['sql_query', 'result_count', 'visualization_size', 
                             'error', 'success', 'query', 'pattern', 'type']:
                        filtered_details[key] = value
                        
                step_info['details'] = filtered_details
                
            debug_info['steps'].append(step_info)
            
        return debug_info
    
    def format_debug_for_display(self) -> Dict[str, Any]:
        """Format debug information for frontend display"""
        debug_info = self.get_debug_info_for_response()
        
        # Add more user-friendly formatting for the frontend
        status_label = "Success" if debug_info['status'] == 'success' else "Error"
        
        formatted_info = {
            'status': debug_info['status'],
            'status_label': status_label,
            'summary': f"{debug_info['completed_steps']}/{debug_info['total_steps']} steps completed successfully in {debug_info['total_duration_ms']:.1f}ms",
            'steps': []
        }
        
        for step in debug_info['steps']:
            # Create a user-friendly status indicator
            if step['status'] == 'completed':
                status_icon = "✓"
                status_class = "success"
            elif step['status'] == 'failed':
                status_icon = "✗"
                status_class = "error"
            else:
                status_icon = "⏳"
                status_class = "warning"
                
            formatted_step = {
                'name': step['name'],
                'status': step['status'],
                'status_icon': status_icon,
                'status_class': status_class,
                'duration': f"{step['duration_ms']:.1f}ms",
                'error': step.get('error'),
                'details': step.get('details', {})
            }
            
            formatted_info['steps'].append(formatted_step)
            
        return formatted_info
    
    def end_flow(self, success: bool = True) -> Dict[str, Any]:
        """End the current flow and return debug info"""
        # End any current step
        if self.current_step:
            self.end_step()
            
        # Get debug info
        debug_info = self.get_debug_info_for_response()
        
        # Override status based on the success parameter
        debug_info['status'] = 'success' if success else 'error'
        
        # Log the flow summary
        self.log_flow_summary()
        
        return debug_info
    
    def add_flow_note(self, note: str) -> None:
        """Add a note to the current flow for debugging purposes"""
        logger.info(f"Flow note: {note}")
        # Create a special system step to record the note
        step = DebugStep(
            name="system_note",
            status="completed",
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_ms=0,
            details={"note": note}
        )
        self.steps.append(step)
    
    def get_steps(self) -> List[Dict[str, Any]]:
        """Get all steps in the flow"""
        return [asdict(step) for step in self.steps]
    
    def get_total_duration(self) -> float:
        """Calculate the total duration of the flow in milliseconds"""
        # If we have a flow start time, calculate from that
        if self.flow_start_time:
            total_time = (datetime.now() - self.flow_start_time).total_seconds() * 1000
            return round(total_time, 2)
        
        # Otherwise sum up the durations of all completed steps
        return round(sum(step.duration_ms or 0 for step in self.steps), 2)
    
    def update_step(self, name: str, details: Optional[Dict[str, Any]] = None) -> None:
        """Update details for a specific step by name"""
        # Find the step by name (most recent matching step)
        target_step = None
        for step in reversed(self.steps):
            if step.name == name:
                target_step = step
                break
                
        if not target_step:
            logger.warning(f"Attempted to update non-existent step: {name}")
            return
            
        # Update details
        if details:
            # Convert string details to dict if needed
            if isinstance(target_step.details, str):
                # If details was a string, convert to dict with the string as a description
                target_step.details = {"description": target_step.details}
            
            # Initialize details if None
            if target_step.details is None:
                target_step.details = {}
                
            # Now we can safely update
            target_step.details.update(details)
            logger.debug(f"Updated step {name} details: {json.dumps(details, indent=2)}") 