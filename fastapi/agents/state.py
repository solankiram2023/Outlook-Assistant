# --- This is the state definition for the AI ---
# --- It defines the state of the agent and the state of the conversation ---

from typing import List, Dict, Optional, TypedDict, Any
from langchain_core.messages import BaseMessage

class AgentState(TypedDict):
    """State for the prompt correction workflow."""
    
    # Conversation history
    messages: List[BaseMessage]
    
    # Raw user input
    current_input: str
    
    # Current email context if available
    email_context: Optional[Dict]
    
    # The improved prompt
    corrected_prompt: Optional[str]
    
    # Which agent should handle this
    # target_agent: Optional[str]
    
    # Additional context, if needed later
    # required_context: Dict[str, Any]